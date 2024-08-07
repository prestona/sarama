package sarama

import (
	"errors"
	"sync"
	"time"
)

func NewAsyncProducer2(addrs []string, conf *Config) (AsyncProducer, error) {
	client, err := NewClient(addrs, conf)
	if err != nil {
		return nil, err
	}
	return newAsyncProducer2(client)
}

func NewAsyncProducerFromClient2(client Client) (AsyncProducer, error) {
	// For clients passed in by the client, ensure we don't
	// call Close() on it.
	cli := &nopCloserClient{client}
	return newAsyncProducer2(cli)
}

type asyncProducer2 struct {
	config           *Config
	errors           chan *ProducerError
	input, successes chan *ProducerMessage

	client Client

	mu             *sync.Mutex
	topicProducers map[string]*topicProducer2
}

func newAsyncProducer2(client Client) (AsyncProducer, error) {
	if client.Closed() {
		return nil, ErrClosedClient
	}

	p := &asyncProducer2{
		config:    client.Config(),
		client:    client,
		errors:    make(chan *ProducerError),
		input:     make(chan *ProducerMessage),
		successes: make(chan *ProducerMessage),
	}
	go p.dispatchInput()
	return p, nil
}

func (ap *asyncProducer2) AsyncClose() { // TODO: wrap in the
	//TODO: go withRecover() ??
	go ap.Close()
}

func (ap *asyncProducer2) Close() error {
	close(ap.input)
	// TODO: need to close the topic producers...
	return nil
}

func (ap *asyncProducer2) Input() chan<- *ProducerMessage {
	return ap.input
}

func (ap *asyncProducer2) Successes() <-chan *ProducerMessage {
	return ap.successes
}

func (ap *asyncProducer2) Errors() <-chan *ProducerError {
	return ap.errors
}

func (ap *asyncProducer2) IsTransactional() bool            { return false }
func (ap *asyncProducer2) TxnStatus() ProducerTxnStatusFlag { return 0 }
func (ap *asyncProducer2) BeginTxn() error                  { return nil }
func (ap *asyncProducer2) CommitTxn() error                 { return nil }
func (ap *asyncProducer2) AbortTxn() error                  { return nil }
func (ap *asyncProducer2) AddOffsetsToTxn(offsets map[string][]*PartitionOffsetMetadata, groupId string) error {
	return nil
}
func (ap *asyncProducer2) AddMessageToTxn(msg *ConsumerMessage, groupId string, metadata *string) error {
	return nil
}

// dispatchInput translates the input channel to a series of calls to the send() method.
// It also ensures that the futures returned by the send method are translated into enqueuing
// results into the appropriate success / error channels. dispatchInput is run using a
// goroutine that is created by newAsyncProducer2().
func (ap *asyncProducer2) dispatchInput() {
	for msg := range ap.input {
		f := ap.send(msg)
		f.onCompletion(func(msg *ProducerMessage, err error) {
			if err != nil {
				ap.errors <- &ProducerError{
					Msg: msg,
					Err: err,
				}
				return
			}
			ap.successes <- msg
		})
	}
}

// send partitions messages by the topic they are being sent to, and passes the
func (ap *asyncProducer2) send(msg *ProducerMessage) *producerFuture {
	tp := ap.topicProducerFor(msg.Topic)
	f := newProducerFuture(msg)
	tp.send(f)
	return f
}

func (ap *asyncProducer2) topicProducerFor(topic string) *topicProducer2 {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	tp := ap.topicProducers[topic]
	if tp == nil {
		tp = newTopicProducer(ap.client, topic)
		ap.topicProducers[topic] = tp
	}
	return tp
}

// ================================================================================
type producerFuture struct {
	msg     *ProducerMessage
	retries int
}

func newProducerFuture(msg *ProducerMessage) *producerFuture {
	return &producerFuture{
		msg: msg,
	}
}

func (pf *producerFuture) onCompletion(f func(msg *ProducerMessage, err error)) {

}

// TODO: is fail a good method name?
func (pf *producerFuture) fail(err error) {

}

// ================================================================================
type topicProducer2 struct {
	config      *Config
	partitioner Partitioner
	client      Client

	mu     *sync.Mutex
	cond   *sync.Cond
	queue  []*producerFuture
	closed bool

	// TODO: doesn't need a lock, as only handled by the dispatch go-routine
	accumulators map[int32]*batchAccumulator
}

func newTopicProducer(client Client, topic string) *topicProducer2 {
	config := client.Config()
	mu := &sync.Mutex{}
	tp := &topicProducer2{
		config:      config,
		client:      client,
		partitioner: config.Producer.Partitioner(topic),
		cond:        sync.NewCond(mu),
	}
	go tp.dispatchTopic()
	return tp
}

func (tp *topicProducer2) send(future *producerFuture) {
	tp.enqueue(future)
	tp.wake()
}

func (tp *topicProducer2) enqueue(future *producerFuture) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	if tp.closed {
		future.fail(ErrClosedClient)
		return
	}
	tp.queue = append(tp.queue, future)
}

func (tp *topicProducer2) wake() {
	tp.cond.Signal()
}

func (tp *topicProducer2) dequeue() *producerFuture {
	tp.cond.L.Lock()
	defer tp.cond.L.Unlock()
	if len(tp.queue) <= 1 {
		tp.queue = nil
		return nil
	}
	tp.queue = tp.queue[1:]
	return tp.queue[0]
}

func (tp *topicProducer2) dispatchTopic() {
	var future *producerFuture
	for {
		if future == nil {
			tp.cond.Wait()
			if tp.closed {
				for _, f := range tp.queue {
					f.fail(ErrClosedClient)
				}
				tp.queue = nil
				tp.cond.L.Unlock()
				for _, acc := range tp.accumulators {
					acc.close()
				}
				tp.accumulators = nil
				return
			}
			if len(tp.queue) > 0 {
				future = tp.queue[0]
			}
			tp.cond.L.Unlock()
		}

		if future == nil {
			continue // TODO: can this actually happen? Probably once we introduce a time aspect to retrying / batching
		}

		retry, err := tp.partition(future)
		if err != nil && retry {
			future.retries++
			if future.retries > tp.config.Producer.Retry.Max {
				future.fail(errors.New("too many retries")) // TODO: better name?
				future = tp.dequeue()
				continue
			} else {
				// TODO: set a timer to wake up on
				future = nil
				continue
			}
		} else if err != nil /* && !retry */ {
			future.fail(err)
			future = tp.dequeue()
			continue
		}

		msg := future.msg
		acc := tp.accumulators[msg.Partition]
		if acc == nil {
			pp := newPartitionProducer2(tp.client, msg.Topic, msg.Partition)
			acc := newBatchAccumulator(pp)
			tp.accumulators[msg.Partition] = acc
		}
		acc.input <- future

		future = tp.dequeue()
	}
}

func (tp *topicProducer2) close() {
	tp.cond.L.Lock()
	tp.closed = true
	tp.cond.L.Unlock()
	tp.wake()
	// TODO: wait until the dispatch go-routine exits
}

func (tp *topicProducer2) partition(future *producerFuture) (bool, error) {
	msg := future.msg
	requiresConsistency := tp.partitioner.RequiresConsistency()
	if dynamic, ok := tp.partitioner.(DynamicConsistencyPartitioner); ok {
		requiresConsistency = dynamic.MessageRequiresConsistency(msg)
	}

	var partitions []int32
	var err error
	if requiresConsistency {
		partitions, err = tp.client.Partitions(msg.Topic)
	} else {
		partitions, err = tp.client.WritablePartitions(msg.Topic)
	}
	if err != nil {
		return true, err
	}

	numPartitions := int32(len(partitions))
	if numPartitions == 0 {
		return true, ErrLeaderNotAvailable
	}

	choice, err := tp.partitioner.Partition(msg, numPartitions)
	if err != nil {
		return false, err
	}
	if choice < 0 || choice >= numPartitions {
		return false, ErrInvalidPartition
	}

	msg.Partition = partitions[choice]
	return false, nil
}

// ================================================================================
type batchAccumulator struct {
	input chan *producerFuture
	pp    *partitionProducer2
}

func newBatchAccumulator(pp *partitionProducer2) *batchAccumulator {
	ba := &batchAccumulator{
		input: make(chan *producerFuture),
		pp:    pp,
	}
	go ba.dispatchToPartition()
	return ba
}

func (ba *batchAccumulator) dispatchToPartition() {
	var current *batch
	var timer *time.Timer
	var timerCh <-chan time.Time
outer:
	for {
		select {
		case future, ok := <-ba.input:
			if !ok {
				break outer
			}
			if current == nil {
				current = newBatch()
				timer = time.NewTimer(100 * time.Millisecond) // TODO: get this value from the config.
				timerCh = timer.C
			}
			current.add(future)

			if current.ready() {
				ba.pp.send(current)
				current = nil
				timer.Stop() // TODO: might be possible to do this more efficiently with restart()
				timerCh = nil
			}
		case <-timerCh:
			ba.pp.send(current)
			current = nil
			timer = nil
			timerCh = nil
		}
	}

	current.err = errors.New("shutting down")
	current.fail()

	ba.pp.close()
}

func (ba *batchAccumulator) close() {
	close(ba.input)
}

// ================================================================================
type partitionProducer2 struct {
	client     Client
	closed     bool
	topic      string
	partition  int32
	maxRetries int
	backoff    time.Duration

	cond    sync.Cond
	current *batch
	ready   []*batch
}

func newPartitionProducer2(client Client, topic string, partition int32) *partitionProducer2 {
	config := client.Config()
	pp := &partitionProducer2{
		client:     client,
		topic:      topic,
		partition:  partition,
		maxRetries: config.Producer.Retry.Max,
		backoff:    config.Producer.Retry.Backoff, // TODO: ignoring backoffFunc
		cond:       *sync.NewCond(&sync.Mutex{}),
		current:    newBatch(),
		ready:      []*batch{},
	}
	go pp.dispatchToPartition()
	return pp
}

func (pp *partitionProducer2) send(batch *batch) {
	closed := false
	pp.cond.L.Lock()
	if pp.closed {
		closed = true
	} else {
		pp.ready = append(pp.ready, batch)
	}
	pp.cond.L.Unlock()

	if closed {
		batch.err = errors.New("closed")
		batch.fail()
	} else {
		pp.wake()
	}
}

func (pp *partitionProducer2) wibble(maxInFlight int) (succeeded, failed, toSend []*batch, refreshLeader bool) {
	newReady := []*batch{}
	inFlight := 0
	for _, batch := range pp.ready {
		if inFlight == maxInFlight {
			return succeeded, failed, toSend, refreshLeader
		}
		switch batch.status {
		case batchNotSent:
			newReady = append(newReady, batch)
			batch.status = batchInFlight
			toSend = append(toSend, batch)
		case batchFailed:
			batch.retries++
			if errors.Is(batch.err, ErrNotLeaderForPartition) {
				// Don't count this against the retries
				batch.retries--
				refreshLeader = true
			} else if errors.Is(batch.err, ErrDuplicateSequenceNumber) {
				// Not really an error - treat as success
				succeeded = append(succeeded, batch)
				continue
			} else if batch.retries > pp.maxRetries {
				failed = append(failed, batch)
				continue
			} else {
				// Assume not retry-able, and refresh the leader
				refreshLeader = true
			}
			newReady = append(newReady, batch)
			batch.status = batchInFlight
			batch.err = nil
			toSend = append(toSend, batch)
		case batchSuccessful:
			succeeded = append(succeeded, batch)
		case batchInFlight:
			newReady = append(newReady, batch)
		}
	}
	pp.ready = newReady

	return succeeded, failed, toSend, refreshLeader
}

func (pp *partitionProducer2) dispatchToPartition() {
	var leader *Broker
	for {
		var succeeded, failed, toSend []*batch
		var refreshLeader bool
		pp.cond.L.Lock()
		for {
			succeeded, failed, toSend, refreshLeader = pp.wibble(999)
			if len(succeeded) == 0 && len(failed) == 0 && len(toSend) == 0 && !refreshLeader {
				// Nothing to do, wait until signalled
				pp.cond.Wait()
				continue
			}
			pp.cond.L.Unlock()
			break
		}

		for _, batch := range succeeded {
			batch.success()
		}

		for _, batch := range failed {
			batch.fail()
		}

		if refreshLeader && leader != nil {
			leader.Close()
			leader = nil
		}

		for _, batch := range toSend {
			if leader == nil {
				var err error
				leader, err = pp.client.Leader(pp.topic, pp.partition)
				if err != nil {
					pp.updateBatchStatus(batch, batchFailed, err)
					time.Sleep(pp.backoff)
					break
				}
			}

			leader.AsyncProduce(batch.toProduceRequest(), func(resp *ProduceResponse, err error) {
				if err != nil {
					pp.updateBatchStatus(batch, batchFailed, err)
				} else if resp.Blocks[pp.topic][pp.partition].Err != ErrNoError {
					pp.updateBatchStatus(batch, batchFailed, resp.Blocks[pp.topic][pp.partition].Err)
				} else {
					pp.updateBatchStatus(batch, batchSuccessful, nil)
				}
				pp.wake()
			})
		}
	}
}

func (pp *partitionProducer2) updateBatchStatus(batch *batch, newStatus batchStatus, err error) {
	pp.cond.L.Lock()
	defer pp.cond.L.Lock()
	batch.status = newStatus
	batch.err = err
}

func (pp *partitionProducer2) wake() {
	pp.cond.Signal()
}

func (pp *partitionProducer2) close() {
	pp.cond.L.Lock()
	pp.closed = true
	pp.cond.L.Unlock()
	pp.wake()
}

// ================================================================================
type batchStatus string

const (
	batchNotSent    = batchStatus("notSent")
	batchInFlight   = batchStatus("inFlight")
	batchSuccessful = batchStatus("successful")
	batchFailed     = batchStatus("failed")
)

type batch struct {
	status  batchStatus
	err     error
	retries int
}

func newBatch() *batch {
	return &batch{}
}

func (b *batch) add(future *producerFuture) {

}

func (b *batch) ready() bool {
	return false
}

func (b *batch) toProduceRequest() *ProduceRequest {
	return nil
}

func (b *batch) fail() {

}

func (b *batch) success() {

}
