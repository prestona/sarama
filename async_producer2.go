package sarama

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"
)

// TODO: random collection of things
// - https://issues.apache.org/jira/browse/KAFKA-5494 - allowed up to 5 in-flights with idempotent producer
// -

// - When idempotence is enabled, the producer fills in the PID field of the batch.
// - It seems like the producer epoch is only relevant when transactions are used.
//   It is incremented for each successive initProducerId call for the same transaction ID.
//   It is used to fence out old producers, if a newer producer starts that uses the same txid.

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

	producerID    int64
	producerEpoch int16

	// TODO: the following are only touched by the goroutine running dispatchInput()
	// TODO: split out into another struct
	awaitingPartitioning map[string]*deque[*producerFuture, producerFuture]
	awaitingLeader       map[string]*deque[*producerFuture, producerFuture]
	accumulator          *batchAccumulator
	inflight             map[int32]*inflightInfo
	drainInflight        bool
	batchTicker          *time.Ticker
	batchTickerRunning   bool
	lastSuccessfulSeqNum map[topicPartition]int64
	nextSequenceNum      map[topicPartition]int64
}

type inflightInfo struct {
	muted   bool
	batches *deque[*batch, batch]
}

func newAsyncProducer2(client Client) (AsyncProducer, error) {
	if client.Closed() {
		return nil, ErrClosedClient
	}
	config := client.Config()

	p := &asyncProducer2{
		config:            config,
		client:            client,
		errors:            make(chan *ProducerError),
		input:             make(chan *ProducerMessage),
		successes:         make(chan *ProducerMessage),
		refreshMetadata:   make(chan string),
		metadataRefreshed: make(chan struct{}),
		waitFor:           make(chan time.Duration),
		produceCompleted:  make(chan *asyncProduceResult),

		producerID: noProducerID,

		awaitingPartitioning: map[string]*deque[*producerFuture, producerFuture]{},
		awaitingLeader:       map[string]*deque[*producerFuture, producerFuture]{},
		accumulator:          netBatchAccumulator(config),
		inflight:             map[int32]*inflightInfo{},
		drainInflight:        false,
		batchTicker:          time.NewTicker(100 * time.Millisecond), // TODO: set this from config
		batchTickerRunning:   false,
		lastSuccessfulSeqNum: map[topicPartition]int64{},
		nextSequenceNum:      map[topicPartition]int64{},
	}
	p.batchTicker.Stop()

	if config.Producer.Idempotent {
		// TODO: for idempotency (and not transactions) the producer ID init call can be made
		// to any broker (which either allocates the producer ID via ZooKeeper or the KRaft quorum).
		// At the point transactions are supported, the init call needs to go to the coordinator that
		// "owns" the transaction ID. So a call to FindCoordinator() (or similar) is required.
		req := &InitProducerIDRequest{}
		// TODO: for transactions we need more things in the request.
		// TODO: it's also possible to send producer epochs (for resuming after something or other), need to determine when to do this...
		resp, err := client.LeastLoadedBroker().InitProducerID(req)
		if err != nil {
			// TODO: need to be more resilient to this failing...
			return nil, err
		}
		p.producerID = resp.ProducerID
		p.producerEpoch = resp.ProducerEpoch
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
	// TODO: in general, need to implement the close logic...
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

func (ap *asyncProducer2) wrapIntoFuture(msg *ProducerMessage) *producerFuture {
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
		for !pq.isEmpty() {
			f := pq.peek()
			if err := ap.partitionMessage(f.msg); err != nil {
				needMetadataRefresh = true
				break
			}
			pq.removeFirst()

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
		for !lq.isEmpty() {
			f := lq.peek()
			broker, leaderEpoch, err := ap.client.LeaderAndEpoch(f.msg.Topic, f.msg.Partition)
			if err != nil {
				needMetadataRefresh = true
				break
			}
			lq.removeFirst()

			ap.accumulator.add(f, broker.ID(), leaderEpoch)
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

	request := b.produceRequest(ap.config, ap.producerEpoch, ap.nextSequenceNum)
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

const maxInflight = 5 // TODO: get this from configuration

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
			inflightForBroker = &inflightInfo{
				batches: newDeque[*batch, batch](),
				muted:   false,
			}
			ap.inflight[brokerID] = inflightForBroker
		}
		for inflightForBroker.batches.size() <= maxInflight {
			b := ap.accumulator.poll(brokerID)
			if b == nil {
				break
			}
			inflightForBroker.batches.add(b)
			ap.sendToBroker(brokerID, b)
		}
	}
}

// updateInflightStatus is called after an Broker.AsyncProduce(...) call completes.
// It marks the corresponding in-flight batch as resolved, and reflects errors reported in
// the produce response back into the in-flight batch.
func updateInflightStatus(produceResult *asyncProduceResult) {
	panicIf(produceResult.batch.resolved, "batch has already been resolved")
	produceResult.batch.resolved = true

	// Response has a top-level error - fail all topic/partitions using this.
	if produceResult.err != nil {
		produceResult.batch.failAll(produceResult.err)
		return
	}

	// Response may have errors for individual topic/partitions - update the response accordingly.
	if produceResult.response != nil {
		for topic, partitionToResponse := range produceResult.response.Blocks {
			for partition, responseBlock := range partitionToResponse {
				// Duplicate sequence numbers are also considered a success, these can occur if
				// there is ambiguity as to whether the broker received a batch, and Sarama retries.
				if responseBlock.Err != ErrNoError &&
					responseBlock.Err != ErrDuplicateSequenceNumber {
					produceResult.batch.fail(topic, partition, responseBlock.Err)
				}
			}
		}
	}
}

// TODO: in general this method is a bit sprawling, and the logic is somewhat haphazardly split
// across int the completeFailedBatches method.
func (ap *asyncProducer2) completeInFlightBatches() {
	for brokerID, brokerInFlight := range ap.inflight {
		// Start at the oldest in-flight batch, skipping any muted brokers:
		// - Remove resolved batches if they were successful
		// - Mute the broker if a resolved batch has failed (but don't remove it)
		// - Stop if an unresolved batch is encountered
		for !brokerInFlight.batches.isEmpty() && !brokerInFlight.muted {
			headBatch := brokerInFlight.batches.peek()
			if headBatch.resolved {
				if headBatch.hasFailures {
					brokerInFlight.muted = true
					break
				} else {
					headBatch.updateLastSuccessfulSequenceNumber(ap.lastSuccessfulSeqNum)
					headBatch.processSuccesses()
					brokerInFlight.batches.removeFirst()
				}
			} else {
				break
			}
		}

		// At this point each in-flight deque should be either:
		// - Empty
		// - Starting with a unresolved batch
		// - Starting with a failed batch (and the corresponding broker is muted)
		batches := brokerInFlight.batches
		panicIf(!batches.isEmpty() && batches.peek().resolved && (!batches.peek().hasFailures || !brokerInFlight.muted),
			"found a non-empty, resolved, non-failed batch at the start of batches for broker %d", brokerID)
	}

	// If all batches for a muted broker are resolved, then retry any retry-able batches and un-mute the broker.
	for brokerID, brokerInFlight := range ap.inflight {
		if !brokerInFlight.muted {
			continue // Only want to process muted brokers
		}

		// Don't proceed until all of the broker's batches are in resolved state
		allResolved := true
		for i := 0; i < brokerInFlight.batches.size(); i++ {
			if !brokerInFlight.batches.get(i).resolved {
				allResolved = false
				break
			}
		}
		if !allResolved {
			continue
		}

		// Remember which topic/partitions might need their sequence numbers reset
		maybeNeedSeqNumReset := map[topicPartition]any{}
		for i := 0; i < brokerInFlight.batches.size(); i++ {
			b := brokerInFlight.batches.get(i)
			for _, tp := range b.topicPartitions() {
				maybeNeedSeqNumReset[tp] = struct{}{}
			}
		}

		// Retry any retry-able batches, empty out the in-flights
		ap.completeFailedBatches(brokerID, brokerInFlight.batches)
		brokerInFlight.batches = newDeque[*batch, batch]()
		brokerInFlight.muted = false // TODO: should the un-mute be done based on a timer?

		// Reset sequence numbers to one beyond the last successful batch's last sequence number
		// TODO: this can only be done once completeFailedBatches has been called because that
		// function also marks partially failed batches as success - however we have to build the list of
		// topic partitions that might need a reset first because completing the batch and invoking
		// callbacks also wipes the partition from the batch. Hence the comment at the start of
		// this method about it being a bit of a jumble.
		for tp := range maybeNeedSeqNumReset {
			v, ok := ap.lastSuccessfulSeqNum[tp]
			if ok {
				ap.nextSequenceNum[tp] = v + 1
			} else {
				ap.nextSequenceNum[tp] = 0
			}
		}
	}
}

func (ap *asyncProducer2) completeFailedBatches(brokerID int32, inflight *deque[*batch, batch]) {
	panicIf(inflight.isEmpty(), "assertion failed: inflight should not be empty")
	panicIf(!inflight.peek().hasFailures, "assertion failed: first inflight should have been marked as failing")

	// Iterate over the inflight batches (starting at the oldest), and remove any topic partitions from the
	// batches that were either successful, or failed with a non-retry-able error (completing the corresponding
	// futures with the appropriate outcome). This leaves batches that contain partitions with retry-able errors.
	isFirst := true
	for idx := 0; idx < inflight.size(); idx++ {
		batch := inflight.get(idx)
		batch.updateLastSuccessfulSequenceNumber(ap.lastSuccessfulSeqNum)
		batch.processSuccesses() // if any partitions completed successfully, then mark their futures as successful, and remove them from the batch
		for tp, err := range batch.topicPartitionErrors {
			if !isRetryable(isFirst, err) {
				batch.processFailures(tp)
			}
		}
		isFirst = false
	}

	// Iterate over the inflight batches (starting at the newest), and re-queue any non-empty batches back
	// into the accumulator (empty batches would correspond to those that either succeeded for all topic partitions,
	// or failed with a non-retry-able error for all topic partitions)
	for idx := inflight.size() - 1; idx >= 0; idx-- {
		batch := inflight.get(idx)
		if len(batch.futures) > 0 {
			batch.resetErrors()
			ap.accumulator.requeue(brokerID, batch)
		}
	}
}

// isRetryable is used to determine whether a batch should be retried or not.
func isRetryable(isFirstFailingInflight bool, err error) bool {
	kerr, ok := err.(KError)
	if !ok {
		return true // Retry errors that weren't returned by Kafka (e.g. connectivity problems)
	}
	if errors.Is(kerr, ErrOutOfOrderSequenceNumber) {
		// Out of order sequence numbers are retry-able if a previous in-flight batch failed,
		// as the broker only tracks sequence numbers for successfully processed batches.
		// They are not retry-able if this error is returned for the first failing in-flight batch.
		return !isFirstFailingInflight
	}
	// Based on the retry-able errors documented in the Kafka Java client's ProduceResponse.java
	return errors.Is(kerr, ErrInvalidMessage) || // ErrInvalidMessage is called CORRUPT_MESSAGE in Java
		errors.Is(kerr, ErrUnknownTopicOrPartition) ||
		errors.Is(kerr, ErrNotLeaderForPartition) ||
		errors.Is(kerr, ErrNotEnoughReplicas) ||
		errors.Is(kerr, ErrNotEnoughReplicasAfterAppend)
}

// dispatchInput...
func (ap *asyncProducer2) dispatchInput() {
	for {
		select {
		case msg := <-ap.input:
			// a new message has been passed to the async producer via its input channel
			// create a future for it
			f := ap.wrapIntoFuture(msg)

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
			ap.completeInFlightBatches()
		}

		ap.maybeProduceBatches()
	}
}

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
type batchAccumulator struct {
	config         *Config
	leaders        map[topicPartition]*leaderInfo
	currentBatches map[int32]*partialBatch // brokerID -> current (incomplete) batch
	readyBatches   map[int32]*batchDeque   //brokerID -> deque of ready batches
}

type leaderInfo struct {
	brokerID    int32
	leaderEpoch int32
}

func netBatchAccumulator(config *Config) *batchAccumulator {
	return &batchAccumulator{
		config:         config,
		leaders:        map[topicPartition]*leaderInfo{},
		currentBatches: map[int32]*partialBatch{},
		readyBatches:   map[int32]*batchDeque{},
	}
}

func (ba *batchAccumulator) add(future *producerFuture, brokerID int32, leaderEpoch int32) {
	tp := topicPartition{topic: future.msg.Topic, partition: future.msg.Partition} // should this be a method on future to return a tp?
	if info, ok := ba.leaders[tp]; ok {
		// TODO: Java code has checks for brokerID and leaderEpoch not being -1. Is that something that can happen here?
		if brokerID != info.brokerID && leaderEpoch > info.leaderEpoch { // Only act on broker changes if epoch is not stale
			// Leader is different to those previously accumulated, remove the partition from any
			// ready batches, and re-add the futures to batches destined for the new broker ID.
			oldBrokerID := info.brokerID
			ba.leaders[tp].brokerID = brokerID
			ba.leaders[tp].leaderEpoch = leaderEpoch

			ready := ba.readyBatches[oldBrokerID]
			for i := 0; i < ready.size(); i++ {
				b := ready.get(i)
				fdq := b.removePartition(future.msg.Topic, future.msg.Partition)
				if fdq == nil {
					continue
				}
				if b.isEmpty() {
					ready.remove(i)
					i-- // TODO: this is a bit of a hack...
				}

				nb := newBatch()
				nb.futures[tp] = fdq
				ready, ok := ba.readyBatches[brokerID]
				if !ok {
					ready := newBatchDeque()
					ba.readyBatches[brokerID] = ready
				}
				ready.add(nb)
			}
		}
	} else {
		ba.leaders[tp] = &leaderInfo{brokerID: brokerID, leaderEpoch: leaderEpoch}
	}

	cb, ok := ba.currentBatches[brokerID]
	if !ok { // TODO: can this pattern be abstracted into a function that uses generics?
		cb = &partialBatch{
			batch: newBatch(),
		}
		ba.currentBatches[brokerID] = cb
	}

	if !cb.add(ba.config, future) {
		// Adding the future to the current batch would overflow
		ba.addReadyBatch(brokerID, ba.currentBatches[brokerID].batch)
		cb = &partialBatch{
			batch: newBatch(),
		}
		ba.currentBatches[brokerID] = cb
	}

	if readyBatch := cb.ready(ba.config); readyBatch != nil {
		// Batch is ready for transmission - add it to the ready batches, and reset the current batch for this broker.
		ba.addReadyBatch(brokerID, readyBatch)
		delete(ba.currentBatches, brokerID)
	}
}

func (ba *batchAccumulator) addReadyBatch(brokerID int32, b *batch) {
	bdq, ok := ba.readyBatches[brokerID]
	if !ok {
		bdq = newBatchDeque()
		ba.readyBatches[brokerID] = bdq
	}
	bdq.add(b)
}

// brokerIDs returns the broker IDs for which the accumulator has ready batches
func (ba *batchAccumulator) brokerIDs() []int32 {
	ids := []int32{}
	for id, fdq := range ba.readyBatches {
		if !fdq.isEmpty() {
			ids = append(ids, id)
		}
	}
	return ids
}

func (ba *batchAccumulator) poll(brokerID int32) *batch {
	ready, ok := ba.readyBatches[brokerID]
	if !ok || ready.isEmpty() {
		return nil
	}
	return ready.removeFirst()
}

func (ba *batchAccumulator) hasIncompleteBatches() bool {
	// TODO: this is used to decide when to start/stop the ticker for time-based batch creation.
	// If we already have available batches, does it make sense to use time-based batch creation?
	return len(ba.currentBatches) != 0
}

func (ba *batchAccumulator) requeue(brokerID int32, b *batch) {
	ready, ok := ba.readyBatches[brokerID]
	if !ok {
		ready = newBatchDeque()
		ba.readyBatches[brokerID] = ready
	}
	ready.addFirst(b)
}

// ================================================================================
type partialBatch struct {
	batch     *batch
	created   time.Time
	sizeMsgs  int
	sizeBytes int
}

// returns true if the future was added to the current batch, or false if adding the future
// would overflow the current batch - i.e. exceed one of the batches hard limits on size or number of messages.
func (cb *partialBatch) add(config *Config, f *producerFuture) bool {
	version := 1
	if config.Version.IsAtLeast(V0_11_0_0) {
		version = 2
	}
	msgBytes := f.msg.ByteSize(version)

	if cb.sizeMsgs == 0 {
		cb.created = time.Now()
		cb.batch = newBatch()                                                  // TODO: check we're not re-initializing this...
		panicIf(msgBytes > int(MaxRequestSize-(10*1024)), "message too large") // TODO: reject messages that are too large before this point...
		return true
	}

	if msgBytes > config.Producer.Flush.Bytes {
		return false
	}
	if config.Producer.Flush.MaxMessages > 0 && cb.sizeMsgs == config.Producer.Flush.MaxMessages {
		return false
	}

	cb.batch.add(f)
	cb.sizeMsgs++
	cb.sizeBytes += msgBytes
	return true
}

// returns the batch, if ready for transmission, or nil if the batch should remain a current
// batch and have more messages added to it.
func (cb *partialBatch) ready(config *Config) *batch {
	if cb.sizeMsgs == 0 {
		return nil // empty batches can never be ready for transmission
	}
	if cb.sizeBytes >= config.Producer.Flush.Bytes {
		return cb.batch
	}
	if cb.sizeBytes >= config.Producer.MaxMessageBytes {
		return cb.batch
	}
	if cb.sizeMsgs >= config.Producer.Flush.MaxMessages {
		return cb.batch
	}
	if cb.created.Add(config.Producer.Flush.Frequency).After(time.Now()) {
		return cb.batch
	}
	return nil
}

// ================================================================================
type producerFuture struct {
	msg  *ProducerMessage
	mu   *sync.Mutex
	done bool
	fn   func(msg *ProducerMessage, err error)
}

func newProducerFuture(msg *ProducerMessage) *producerFuture {
	return &producerFuture{
		msg: msg,
		mu:  &sync.Mutex{},
	}
}

func (pf *producerFuture) onCompletion(fn func(msg *ProducerMessage, err error)) {
	pf.mu.Lock()
	defer pf.mu.Unlock()
	panicIf(pf.done, "onCompletion registered for completed future")
	panicIf(pf.fn != nil, "multiple calls to onCompletion for same future")
	pf.fn = fn
}

func (pf *producerFuture) complete(err error) {
	func() {
		pf.mu.Lock()
		defer pf.mu.Unlock()
		panicIf(pf.done, "duplicate call to complete future")
		panicIf(pf.done, "no onCompletion callback registered with future")
		pf.done = true
	}()
	pf.fn(pf.msg, err)
}

// ================================================================================

// deque is a double-ended queue, somewhat inspired by Java's Deque class.
// The implementation is very basic, and might not make efficient use of the
// memory backing the slice of elements held by the deque.
// Trying to perform invalid operations (such as removing an item from the head
// of an empty deque) will panic (by translating the operation to a invalid operation
// on the underlying slice). This is a deliberate choice, as typically there will be
// no obvious recovery action that can be performed if the code finds itself in an
// inconsistent state.
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

func (d *deque[T, Q]) addFirst(v ...T) {
	d.elements = append(v, d.elements...)
}

func (d *deque[T, Q]) peek() T {
	return d.elements[len(d.elements)-1]
}

func (d *deque[T, Q]) removeFirst() T {
	v := d.peek()
	d.elements = d.elements[:len(d.elements)-1]
	return v
}

func (d *deque[T, Q]) remove(i int) T {
	v := d.elements[i]
	d.elements = append(d.elements[:i], d.elements[i+1:]...)
	return v
}

func (d *deque[T, Q]) isEmpty() bool {
	return len(d.elements) == 0
}

func (d *deque[T, Q]) size() int {
	return len(d.elements)
}

func (d *deque[T, Q]) get(idx int) T {
	return d.elements[idx]
}

// ================================================================================
type futureDeque struct {
	deque[*producerFuture, producerFuture]
}

func newFutureDeque(f ...*producerFuture) *futureDeque {
	return &futureDeque{
		deque: *newDeque[*producerFuture, producerFuture](f...),
	}
}

// ================================================================================
type batchDeque struct {
	deque[*batch, batch]
}

func newBatchDeque(b ...*batch) *batchDeque {
	return &batchDeque{
		*newDeque[*batch, batch](b...),
	}
}

// ================================================================================
type batch struct {
	// topic name -> partition idx -> deque of futures
	futures map[topicPartition]*futureDeque
	// topic name -> partition idx -> error
	topicPartitionErrors map[topicPartition]error // TODO: should this be named 'errors'?
	hasFailures          bool
	resolved             bool // TODO: not a good name - when this is set to true it means that we've determined the outcome of sending this batch.
	firstSequenceNum     map[topicPartition]int64
}

func newBatch() *batch {
	return &batch{
		futures:              nil,
		topicPartitionErrors: map[topicPartition]error{},
		firstSequenceNum:     map[topicPartition]int64{},
	}
}

func (b *batch) add(future *producerFuture) {
	if b.futures == nil {
		b.futures = map[topicPartition]*futureDeque{}
	}
	tp := topicPartition{topic: future.msg.Topic, partition: future.msg.Partition}
	if _, ok := b.futures[tp]; !ok {
		b.futures[tp] = newFutureDeque()
	}
	b.futures[tp].add(future)
}

func (b *batch) failAll(err error) {
	b.hasFailures = true
	for tp := range b.futures {
		b.topicPartitionErrors[tp] = err
	}
}

func (b *batch) fail(topic string, partition int32, err KError) {
	b.hasFailures = true
	tp := topicPartition{topic: topic, partition: partition}
	b.topicPartitionErrors[tp] = err
}

// processSuccesses successfully completes any futures in the batch that do not have
// an error associated with the topic partition. These topic partitions are also removed
// from the futures map, leaving only topic partitions that have errors associated with them.
func (b *batch) processSuccesses() {
	for tp, fdq := range b.futures {
		if b.topicPartitionErrors[tp] != nil {
			continue
		}
		for !fdq.isEmpty() {
			f := fdq.removeFirst()
			f.complete(nil)
		}
		delete(b.futures, tp)
	}
}

func (b *batch) updateLastSuccessfulSequenceNumber(lastSuccessfulSeqNum map[topicPartition]int64) {
	for tp, fdq := range b.futures {
		if b.topicPartitionErrors[tp] != nil {
			continue
		}
		lastSuccess := b.firstSequenceNum[tp] + int64(fdq.size())
		v, ok := lastSuccessfulSeqNum[tp]
		if !ok || lastSuccess > v {
			lastSuccessfulSeqNum[tp] = lastSuccess
		}
	}
}

// processFailures fails the futures associated with the topic partition. This topic partition
// is then removed from the map of topic partition -> futures held in the batch.
func (b *batch) processFailures(tp topicPartition) {
	fdq := b.futures[tp]
	err := b.topicPartitionErrors[tp]
	panicIf(fdq == nil, "could not find future for tp=%v", tp)
	panicIf(err == nil, "could not find error for tp=%v", tp)

	for !fdq.isEmpty() {
		f := fdq.removeFirst()
		f.complete(err)
	}
	delete(b.futures, tp)
}

func (b *batch) resetErrors() {
	b.hasFailures = false
	b.topicPartitionErrors = map[topicPartition]error{}
}

func (b *batch) removePartition(topic string, partition int32) *futureDeque {
	tp := topicPartition{topic: topic, partition: partition}
	fdq := b.futures[tp]
	delete(b.futures, tp)
	delete(b.topicPartitionErrors, tp) // TODO: this probably isn't necessary...
	return fdq
}

func (b *batch) isEmpty() bool {
	return len(b.futures) == 0
}

func (b *batch) produceRequest(config *Config, producerEpoch int16, sequence map[topicPartition]int64) *ProduceRequest {
	// TODO: for the moment, we only care about building the version 2 batch format.
	pr := &ProduceRequest{
		RequiredAcks: config.Producer.RequiredAcks,
		Timeout:      int32(config.Producer.Timeout / time.Millisecond),
		records:      map[string]map[int32]Records{},
	}
	switch {
	case config.Version.IsAtLeast(V2_1_0_0):
		pr.Version = 7
	case config.Version.IsAtLeast(V2_0_0_0):
		pr.Version = 6
	case config.Version.IsAtLeast(V1_0_0_0):
		pr.Version = 5
	case config.Version.IsAtLeast(V0_11_0_0):
		pr.Version = 3
	case config.Version.IsAtLeast(V0_10_0_0):
		pr.Version = 2
	}
	// TODO: if version IsAtLeast(V0_11_0_0) - need to consider setting the transaction ID

	// pr contains records = map[string]map[int32]Records
	// Records contains a records batch (of type RecordBatch
	// RecordBatch contains an array of []Record
	for tp, fdq := range b.futures {
		panicIf(fdq.size() == 0, "encountered empty future queue for topic/partition")
		recordSlice := make([]*Record, fdq.size())
		for i := 0; i < fdq.size(); i++ {
			f := fdq.get(i)
			recordSlice[i] = recordFor(i == 0, f.msg)
		}
		rb := &RecordBatch{
			Version:              2,
			PartitionLeaderEpoch: 0, // TODO: this is never used by Sarama (but the broker expects it)...
			ProducerEpoch:        producerEpoch,
			Codec:                config.Producer.Compression,
			CompressionLevel:     config.Producer.CompressionLevel,
			// TODO: presumably there are other important fields in here...
			Records: recordSlice,
			// FirstOffset is set below...
		}
		if config.Producer.Idempotent {
			rb.FirstOffset = sequence[tp]
			sequence[tp] += int64(len(rb.Records))
		}
		records := newDefaultRecords(rb)
		partitionToRecords, ok := pr.records[tp.topic]
		if !ok {
			partitionToRecords = map[int32]Records{}
			pr.records[tp.topic] = partitionToRecords
		}
		partitionToRecords[tp.partition] = records
	}

	return nil // TODO: write the code for this.
}

func recordFor(isFirst bool, msg *ProducerMessage) *Record {
	size := 0 // TODO: size is used to provide a more accurate "is this batch big enough?" check, and also when dropping partitions from a set
	if isFirst {
		size += recordBatchOverhead
	}

	var err error
	var key, val []byte

	// TODO: given that these can fail - it would make sense to do this much earlier in the
	// async producer. Maybe the future type should encapsulate a Record rather than a ProducerMessage?
	if msg.Key != nil {
		key, err = msg.Key.Encode()
		panicIf(err != nil, "couldn't encode key")
	}
	if msg.Value != nil {
		val, err = msg.Value.Encode()
		panicIf(err != nil, "couldn't encode value")
	}

	size += maximumRecordOverhead
	record := &Record{
		Key:            key,
		Value:          val,
		TimestampDelta: 0, // TODO: timestamp.Sub(set.recordsToSend.RecordBatch.FirstTimestamp),
		OffsetDelta:    0, // TODO: need to work out how to set this...
		// Headers set below
		// TODO: do Attributes need to be set?
	}
	if len(msg.Headers) > 0 {
		record.Headers = make([]*RecordHeader, len(msg.Headers))
		for i := range msg.Headers {
			record.Headers[i] = &msg.Headers[i]
			size += len(record.Headers[i].Key) + len(record.Headers[i].Value) + 2*binary.MaxVarintLen32
		}
	}
	return record
}

func (b *batch) topicPartitions() []topicPartition { // TODO: this doesn't seem like a good interface to provide...
	result := []topicPartition{}
	for tp := range b.futures {
		result = append(result, tp)
	}
	return result
}

// ================================================================================

func panicIf(b bool, msg string, v ...any) {
	if b {
		panic(fmt.Sprintf(msg, v...))
	}
}
