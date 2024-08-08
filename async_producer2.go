package sarama

import "time"

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
	config            *Config
	errors            chan *ProducerError
	input, successes  chan *ProducerMessage
	refreshMetadata   chan string
	metadataRefreshed chan struct{}
	waitFor           chan time.Duration
	produceCompleted  chan *asyncProduceResult
	client            Client

	// TODO: the following are only touched by the goroutine running dispatchInput()
	// TODO: split out into another struct
	awaitingPartitioning map[string]*deque[*producerFuture, producerFuture]
	awaitingLeader       map[string]*deque[*producerFuture, producerFuture]
	accumulator          *batchAccumulator
	inflight             map[int32]*deque[*batch, batch]
	drainInflight        bool
	batchTicker          *time.Ticker
	batchTickerRunning   bool
}

func newAsyncProducer2(client Client) (AsyncProducer, error) {
	if client.Closed() {
		return nil, ErrClosedClient
	}

	p := &asyncProducer2{
		config:            client.Config(),
		client:            client,
		errors:            make(chan *ProducerError),
		input:             make(chan *ProducerMessage),
		successes:         make(chan *ProducerMessage),
		refreshMetadata:   make(chan string),
		metadataRefreshed: make(chan struct{}),
		waitFor:           make(chan time.Duration),
		produceCompleted:  make(chan *asyncProduceResult),

		awaitingPartitioning: map[string]*deque[*producerFuture, producerFuture]{},
		awaitingLeader:       map[string]*deque[*producerFuture, producerFuture]{},
		accumulator:          netBatchAccumulator(),
		inflight:             map[int32]*deque[*batch, batch]{},
		drainInflight:        false,
		batchTicker:          time.NewTicker(100 * time.Millisecond), // TODO: set this from config
		batchTickerRunning:   false,
	}
	p.batchTicker.Stop()

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

// TODO: rename something like "wrapWithFuture"
func (ap *asyncProducer2) setupFuture(msg *ProducerMessage) *producerFuture {
	f := newProducerFuture(msg)
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
	return f
}

// TODO: currently there's no escape from this if a message persistently can't be partitioned
func (ap *asyncProducer2) processAwaitingPartitioning() bool {
	needMetadataRefresh := false
	for _, pq := range ap.awaitingPartitioning {
		for !pq.empty() {
			f := pq.peek()
			if err := ap.partitionMessage(f.msg); err != nil {
				needMetadataRefresh = true
				break
			}
			pq.remove()

			if lq, ok := ap.awaitingLeader[f.msg.Topic]; !ok {
				ap.awaitingLeader[f.msg.Topic] = newDeque[*producerFuture, producerFuture](f)
			} else {
				lq.add(f)
			}
		}
	}

	return needMetadataRefresh
}

// TODO: currently there's no escape from this if a message persistently can't find a leader
func (ap *asyncProducer2) processAwaitingLeader() bool {
	needMetadataRefresh := false
	for _, lq := range ap.awaitingLeader {
		for !lq.empty() {
			f := lq.peek()
			broker, brokerEpoc, err := ap.client.LeaderAndEpoch(f.msg.Topic, f.msg.Partition)
			if err != nil {
				needMetadataRefresh = true
				break
			}
			lq.remove()

			ap.accumulator.add(f, broker.ID(), brokerEpoc)
		}
	}

	return needMetadataRefresh
}

func (ap *asyncProducer2) sendToBroker(brokerID int32, b *batch) {
	broker, err := ap.client.Broker(brokerID)
	if err != nil {
		// Handle all outcomes in the same way: another goroutine invoking asyncProducerCallback.
		go ap.asyncProduceCallback(b, nil, err)
	}
	request := &ProduceRequest{}
	// TODO: now need to think about how to assign sequence numbers to batches.

	err = broker.AsyncProduce(request, func(resp *ProduceResponse, err error) {
		ap.asyncProduceCallback(b, resp, err)
	})
	if err != nil || request.RequiredAcks == NoResponse {
		// AsyncProduce doesn't invoke the callback for acks=0 so make sure asyncProduceCallback
		// is notified that the produce has been attempted.
		go ap.asyncProduceCallback(b, nil, err)
	}
}

// asyncProduceCallback is called in response to producing a message using the Broker.AsyncProduce(...) method.
// As this method is used across a number of Brokers, it can be called concurrently on multiple goroutines.
// The response argument can be nil if the produce request was with acks=0
func (ap *asyncProducer2) asyncProduceCallback(batch *batch, response *ProduceResponse, err error) {
	ap.produceCompleted <- &asyncProduceResult{
		batch:    batch,
		response: response,
		err:      err,
	}
}

type asyncProduceResult struct {
	batch    *batch
	response *ProduceResponse
	err      error
}

const maxInflight = 3 // TODO: get this from configuration

func (ap *asyncProducer2) maybeProduceBatches() {
	// Try to assign partitions to any futures awaiting partitioning, and find leaders for any awaiting a leader
	updateMetadata := ap.processAwaitingPartitioning()
	updateMetadata = updateMetadata || ap.processAwaitingLeader()
	if updateMetadata {
		// If some futures couldn't be partitioned or don't have a leader, trigger a metadata refresh.
		ap.refreshMetadata <- "x" // TODO: wrong type for channel? Or wrong return type from processX functions?
	}

	// TODO: tickers will panic if passed a zero duration - is that ever a valid configuration for Sarama?
	if ap.accumulator.hasIncompleteBatches() && !ap.batchTickerRunning {
		ap.batchTicker.Reset(100 * time.Millisecond) // TODO: get this from config
		ap.batchTickerRunning = true                 // TODO: maybe wrap this and the ticker into a struct to make tracking this easier...
	} else if !ap.accumulator.hasIncompleteBatches() && ap.batchTickerRunning {
		ap.batchTicker.Stop()
		ap.batchTickerRunning = false
	}

	// If the in-flight messages are being drained, then there is nothing left to do.
	// Transitioning out of draining will occur when notified that the last of the batches has been processed
	if ap.drainInflight {
		return
	}

	// See if there is capacity to move accumulated batches into inflight.
	for _, brokerID := range ap.accumulator.brokerIDs() {
		inflightForBroker, ok := ap.inflight[brokerID]
		if !ok {
			inflightForBroker = newDeque[*batch, batch]()
			ap.inflight[brokerID] = inflightForBroker
		}
		for inflightForBroker.size() <= maxInflight {
			b := ap.accumulator.poll(brokerID)
			if b == nil {
				break
			}
			inflightForBroker.add(b)
			ap.sendToBroker(brokerID, b)
		}
	}
}

func updateInflightStatus(produceResult *asyncProduceResult) {
	if produceResult.batch.resolved {
		panic("batch has already been resolved!") // TODO: convert to log line (or something) once the code has been debugged.
	}
	if produceResult.err != nil {
		produceResult.batch.failAll(produceResult.err)
		return
	}
	if produceResult.response != nil {
		// TODO: work out what to do based on the individual responses.
		for topic, partitionToResponse := range produceResult.response.Blocks {
			for partition, responseBlock := range partitionToResponse {
				if responseBlock.Err != ErrNoError &&
					responseBlock.Err != ErrDuplicateSequenceNumber { // TODO: explain the significance of this...
					produceResult.batch.fail(topic, partition, responseBlock.Err)
				}
			}
		}
	}
	produceResult.batch.resolved = true
}

func (ap *asyncProducer2) maybeRemoveInFlightBatches() { // TODO: complete might be a better term?
	// TODO: where are we toggling the drain flag?

	for _, batches := range ap.inflight {
		for !batches.empty() {

		}
	}
}

// dispatchInput...
func (ap *asyncProducer2) dispatchInput() {
	for {
		select {
		case msg := <-ap.input:
			// a new message has been passed to the async producer via its input channel
			// create a future for it
			f := ap.setupFuture(msg)

			// Add the future to those awaiting partitioning
			if pq, ok := ap.awaitingPartitioning[msg.Topic]; !ok {
				ap.awaitingPartitioning[msg.Topic] = newDeque[*producerFuture, producerFuture](f)
			} else {
				pq.add(f)
			}

		case <-ap.metadataRefreshed:
			// metadata has been refreshed - see if this allows for more batches to be ready
			// to produce

		case <-ap.batchTicker.C:
			// deadline for time based batch completion has been reached - see if this has
			// caused more batches to become ready to produce.

		case produceResult := <-ap.produceCompleted:
			updateInflightStatus(produceResult)
			ap.maybeRemoveInFlightBatches()
		}

		ap.maybeProduceBatches()
	}
}

// func (ap *asyncProducer2) reattemptPartitioning(map[string])

func (ap *asyncProducer2) partitionMessage(msg *ProducerMessage) error {
	partitioner := ap.config.Producer.Partitioner(msg.Topic) // TODO: building this on each call is inefficient...
	var partitions []int32

	requiresConsistency := false
	if ep, ok := partitioner.(DynamicConsistencyPartitioner); ok {
		requiresConsistency = ep.MessageRequiresConsistency(msg)
	} else {
		requiresConsistency = partitioner.RequiresConsistency()
	}

	var err error
	if requiresConsistency {
		partitions, err = ap.client.Partitions(msg.Topic)
	} else {
		partitions, err = ap.client.WritablePartitions(msg.Topic)
	}
	if err != nil {
		return err
	}

	numPartitions := int32(len(partitions))
	if numPartitions == 0 {
		return ErrLeaderNotAvailable
	}

	choice, err := partitioner.Partition(msg, numPartitions)

	if err != nil {
		return err
	} else if choice < 0 || choice >= numPartitions {
		return ErrInvalidPartition // TODO: some of these errors should be hard failures for the message.
	}

	msg.Partition = partitions[choice]

	return nil
}

// ================================================================================

// ><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><
// Need to change how this works:
// 1. Accumulator should internally track records by a deque keyed on topic/partition
// ** actually will doing 1. cause any ordering / fairness problems? **
// 2. It should also remember the broker ID associated with a topic/partition, and respond if this is ever changed.
//    Specifically: it should throw away any batches that it hasn't emitted for the changed broker
// 3. Getting batches out of the accumulator (e.g. poll) should be done one at a time (or with a bound) and
//    be polled per-broker ID. This might make determining the time to wait until there might be a completed batch
//    a bit more difficult.
// ><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

type batchAccumulator struct{}

func netBatchAccumulator() *batchAccumulator {
	return &batchAccumulator{}
}

func (ba *batchAccumulator) add(future *producerFuture, brokerID int32, brokerEpoc int32) {

}

// brokerIDs returns the broker IDs for which the accumulator has ready batches
func (ba *batchAccumulator) brokerIDs() []int32 {
	return nil
}

// poll() returns:
// - a map of topic -> partition -> deque of messages (can be nil if there aren't any ready batches)
func (ba *batchAccumulator) poll(brokerID int32) *batch {
	return nil
}

func (ba *batchAccumulator) hasIncompleteBatches() bool {
	return false
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
type deque[T *Q, Q any] struct {
	elements []T
}

func newDeque[T *Q, Q any](vs ...T) *deque[T, Q] {
	elements := make([]T, len(vs))
	copy(elements, vs)
	return &deque[T, Q]{
		elements: elements,
	}
}

func (d *deque[T, Q]) add(v ...T) {
	d.elements = append(d.elements, v...)
}

func (d *deque[T, Q]) peek() T {
	if len(d.elements) == 0 {
		// TODO: should this be an error?
		return nil
	}
	return d.elements[len(d.elements)-1]
}

func (d *deque[T, Q]) remove() T {
	if len(d.elements) == 0 {
		// TODO: should this be an error?
		return nil
	}
	v := d.peek()
	d.elements = d.elements[:len(d.elements)-1]
	return v
}

func (d *deque[T, Q]) empty() bool {
	return len(d.elements) == 0
}

func (d *deque[T, Q]) size() int {
	return len(d.elements)
}

func (d *deque[T, Q]) get(idx int) T {
	// TODO: no bounds check, we just panic.
	return d.elements[idx]
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
	// topic name -> partition idx -> deque of futures
	futures map[string]map[int32]*deque[*producerFuture, producerFuture]
	// topic name -> partition idx -> KError
	topicPartitionErrors map[string]map[int32]KError
	// top level err. If this is set then all topic/partitions failed.
	err         error
	hasFailures bool
	created     time.Time
	resolved    bool // TODO: not a good name - means that we've determined the outcome of sending this batch.
}

func newBatch() *batch {
	return &batch{
		futures:              nil,
		topicPartitionErrors: map[string]map[int32]KError{},
	}
}

func (b *batch) add(future *producerFuture) {
	if b.futures == nil {
		b.created = time.Now()
		b.futures = map[string]map[int32]*deque[*producerFuture, producerFuture]{}
	}
	partitionToFutures, ok := b.futures[future.msg.Topic]
	if !ok {
		partitionToFutures = map[int32]*deque[*producerFuture, producerFuture]{}
		b.futures[future.msg.Topic] = partitionToFutures
	}
	q, ok := partitionToFutures[future.msg.Partition]
	if !ok {
		q = newDeque[*producerFuture, producerFuture]()
		partitionToFutures[future.msg.Partition] = q
	}
	q.add(future)
}

func (b *batch) failAll(err error) {
	b.err = err
	b.hasFailures = true
}

func (b *batch) fail(topic string, partition int32, err KError) {
	b.hasFailures = true
	partitionToError, ok := b.topicPartitionErrors[topic]
	if !ok {
		partitionToError = map[int32]KError{}
		b.topicPartitionErrors[topic] = partitionToError
	}
	partitionToError[partition] = err
}
