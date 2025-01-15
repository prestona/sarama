package sarama

import (
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
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

	// TODO: the following are only touched by the goroutine running eventLoop()
	// TODO: split out into another struct
	awaitingPartitioning map[string]*futureDeque
	awaitingLeader       map[topicPartition]*futureDeque
	accumulator          *batchAccumulator
	inflight             map[int32]*inflightInfo
	batchTicker          *time.Ticker
	batchTickerRunning   bool
	lastSuccessfulSeqNum map[topicPartition]int32
	nextSequenceNum      map[topicPartition]int32
	unMuteTimer          *timer[int] // TODO: does this even need a type?
	mutedTopics          *mutedSet[string]
	mutedTopicPartitions *mutedSet[topicPartition]
	mutedBrokers         *mutedSet[int32]

	// TODO: the following relate to transactions - should they be in their own struct?
	txID              string // empty if no transaction ID set
	idempotent        bool
	txState           txState
	flushTx           chan error
	txProducerEpoch   int16                       // TODO: how does this differ from the produceEpoch field?
	txAbortErr        error                       // set if a transaction becomes abort only
	txAddedPartitions map[topicPartition]struct{} // set of partitions already added to current tx
	txNewPartitions   map[topicPartition]struct{} // set of partitions not yet added to current tx
}

type inflightInfo struct {
	muted   bool
	batches *batchDeque
}

func (ap *asyncProducer2) abortableError(err error) {
	if ap.txAbortErr != nil {
		ap.txAbortErr = err
	}
}

func (ap *asyncProducer2) fatalError(err error) {
	ap.abortableError(err)
	// TODO: need to mark the async producer as having received a fatal error.
}

func newAsyncProducer2(client Client) (AsyncProducer, error) {
	if client.Closed() {
		return nil, ErrClosedClient
	}
	config := client.Config()

	unMuteTimer := newTimer[int]()
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

		awaitingPartitioning: map[string]*futureDeque{},
		awaitingLeader:       map[topicPartition]*futureDeque{},
		accumulator:          netBatchAccumulator(config),
		inflight:             map[int32]*inflightInfo{},
		batchTicker:          time.NewTicker(100 * time.Millisecond), // TODO: set this from config
		batchTickerRunning:   false,
		lastSuccessfulSeqNum: map[topicPartition]int32{},
		nextSequenceNum:      map[topicPartition]int32{},
		unMuteTimer:          unMuteTimer,
		mutedTopics:          newMutedSet[string](unMuteTimer),
		mutedTopicPartitions: newMutedSet[topicPartition](unMuteTimer),
		mutedBrokers:         newMutedSet[int32](unMuteTimer),
		txAddedPartitions:    map[topicPartition]struct{}{},
		txNewPartitions:      map[topicPartition]struct{}{},
	}
	p.batchTicker.Stop() // annoyingly Go tickers can't be created in a stopped state

	if config.Producer.Idempotent {
		p.idempotent = true
		if config.Producer.Transaction.ID != "" {
			p.txID = config.Producer.Transaction.ID
			p.txState = txStateUninitialized
		} else {
			// If idempotent, but not transacted - then initialize the producer ID
			// at the point the async producer is created
			req := &InitProducerIDRequest{}
			resp, err := client.LeastLoadedBroker().InitProducerID(req)
			if err != nil {
				// TODO: need to be more resilient to this failing...
				// TODO: for example try it more than once
				return nil, err
			}
			if resp.Err != ErrNoError {
				// TODO: should probably re-try if the error can be re-tried.
				return nil, resp.Err
			}
			p.producerID = resp.ProducerID
			p.producerEpoch = resp.ProducerEpoch
		}
	}

	go p.eventLoop()
	return p, nil
}

func (ap *asyncProducer2) AsyncClose() {
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

// TODO: this still needs to be implemented.
// TODO: what are the pro's and con's of also dispatching this to the goroutine
// running the event loop?
func (ap *asyncProducer2) TxnStatus() ProducerTxnStatusFlag { return 0 }

func (ap *asyncProducer2) IsTransactional() bool { return ap.txID != "" }
func (ap *asyncProducer2) BeginTxn() error       { return ap.doTxnOperation(txFlagBegin) }
func (ap *asyncProducer2) CommitTxn() error      { return ap.doTxnOperation(txFlagCommit) }
func (ap *asyncProducer2) AbortTxn() error       { return ap.doTxnOperation(txFlagAbort) }

func (ap *asyncProducer2) doTxnOperation(flags txFlags) error {
	if ap.txID == "" {
		// TODO: is this the right error type to return?
		return errors.New("producer is not transactional")
	}
	errCh := make(chan error)
	defer close(errCh)
	ap.input <- &ProducerMessage{
		flags2:   flags,
		txResult: errCh,
	}
	return <-errCh
}

func (ap *asyncProducer2) AddOffsetsToTxn(offsets map[string][]*PartitionOffsetMetadata, groupId string) error {
	// TODO: this is a copy of doTxnOperation, can it be made more common?
	if ap.txID == "" {
		// TODO: is this the right error type to return?
		return errors.New("producer is not transactional")
	}
	errCh := make(chan error)
	defer close(errCh)
	ap.input <- &ProducerMessage{
		flags2:              txFlagAddOffsets,
		txAddOffsets:        offsets,
		txAddOffsetsGroupId: groupId,
		txResult:            errCh,
	}
	return <-errCh
}

func (ap *asyncProducer2) AddMessageToTxn(msg *ConsumerMessage, groupId string, metadata *string) error {
	return ap.AddOffsetsToTxn(map[string][]*PartitionOffsetMetadata{
		msg.Topic: {
			{
				Partition: msg.Partition,
				Offset:    msg.Offset + 1,
				Metadata:  metadata,
			},
		},
	}, groupId)
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

// TODO: this is a place-holder until a proper retry time calculation is implemented.
const retryDuration = 5 * time.Second

// associatePartitionWithTransaction records that the specified partition is part of the current
// transaction. This is used to ensure that a 'AddPartitionsToTxnRequest' is sent to the transaction
// coordinator prior to producing the first record to the partition.
func (ap *asyncProducer2) associatePartitionWithTransaction(tp *topicPartition) {
	if _, ok := ap.txAddedPartitions[*tp]; !ok { // Skip if an AddPartitionToTxnRequest has already been sent
		ap.txNewPartitions[*tp] = struct{}{}
	}
}

// TODO: currently there's no escape from this if a message persistently can't be partitioned
func (ap *asyncProducer2) processAwaitingPartitioning() {
	for topic, pq := range ap.awaitingPartitioning {
		// If the transaction is aborting, then fail any queued messages
		if ap.IsTransactional() && ap.txAbortErr != nil {
			for !pq.isEmpty() {
				f := pq.removeFirst()
				f.complete(ap.txAbortErr)
			}
			continue
		}

		if ap.mutedTopics.contains(topic) {
			continue // skip muted topics
		}

		for !pq.isEmpty() {
			f := pq.peek()
			retry, err := ap.partitionMessage(f.msg)
			if err != nil && retry {
				ap.mutedTopics.add(topic, retryDuration)
				break
			}

			pq.removeFirst()

			if err != nil && !retry {
				f.complete(err) // Fail the message
				continue
			}

			tp := topicPartition{f.msg.Topic, f.msg.Partition}
			if ap.txID != "" {
				ap.associatePartitionWithTransaction(&tp)
			}
			if lq, ok := ap.awaitingLeader[tp]; !ok {
				ap.awaitingLeader[tp] = newFutureDeque(f)
			} else {
				lq.add(f)
			}
		}
	}
}

// TODO: currently there's no escape from this if a message persistently can't find a leader
func (ap *asyncProducer2) processAwaitingLeader() {
	for tp, lq := range ap.awaitingLeader {
		// If the transaction is aborting, then fail any queued messages
		if ap.IsTransactional() && ap.txAbortErr != nil {
			for !lq.isEmpty() {
				f := lq.removeFirst()
				f.complete(ap.txAbortErr)
			}
			continue
		}

		if ap.mutedTopicPartitions.contains(tp) {
			continue
		}

		for !lq.isEmpty() {
			f := lq.peek()
			broker, leaderEpoch, err := ap.client.LeaderAndEpoch(f.msg.Topic, f.msg.Partition)
			if err != nil {
				ap.mutedTopicPartitions.add(tp, retryDuration)
				break
			}
			lq.removeFirst()

			ap.accumulator.add(f, broker.ID(), leaderEpoch)
		}
	}
}

// TODO: this method name isn't very good. The purpose is to determine if the producer
// is buffering any messages that haven't yet been fully sent to the broker. The code
// for ending a transaction is interested in determining this, as when commit / abort are
// called, the producer needs to flush any buffered records before ending the transaction.
func (ap *asyncProducer2) isEmpty() bool {
	for _, pq := range ap.awaitingPartitioning {
		if !pq.isEmpty() {
			return false
		}
	}
	for _, lq := range ap.awaitingLeader {
		if !lq.isEmpty() {
			return false
		}
	}
	if !ap.accumulator.isEmpty() {
		return false
	}
	for _, i := range ap.inflight {
		if i != nil && !i.batches.isEmpty() {
			return false
		}
	}
	return true
}

func (ap *asyncProducer2) sendToBroker(brokerID int32, b *batch) {
	fmt.Printf("sending batch %p to broker %d\n", b, brokerID)
	broker, err := ap.client.Broker(brokerID)
	if err != nil {
		// Handle all outcomes in the same way: another goroutine invoking asyncProducerCallback.
		go ap.asyncProduceCallback(brokerID, b, nil, err)
		return
	}

	request := b.produceRequest(ap.config, ap.producerID, ap.producerEpoch, ap.nextSequenceNum)
	err = broker.AsyncProduce(request, func(resp *ProduceResponse, err error) {
		// This call to asyncProducerCallback will be invoked on a thread run by the broker
		ap.asyncProduceCallback(brokerID, b, resp, err)
	})
	if err != nil || request.RequiredAcks == NoResponse {
		// AsyncProduce doesn't invoke the callback for acks=0 so make sure asyncProduceCallback
		// is notified that the produce has been attempted.
		go ap.asyncProduceCallback(brokerID, b, nil, err)
		return
	}
}

// asyncProduceCallback is called in response to producing a message using the Broker.AsyncProduce(...) method.
// As this method is used across a number of Brokers, it can be called concurrently on multiple goroutines.
// The response argument can be nil if the produce request was with acks=0
func (ap *asyncProducer2) asyncProduceCallback(brokerID int32, batch *batch, response *ProduceResponse, err error) {
	fmt.Printf("asyncProduceCallback %v(%p) %v %v\n", batch, batch, response, err)

	if err != nil {
		// If the broker needs to be closed, then launch this in a separate go-routine that also delivers the result
		// once the close has completed. This is necessary to avoid a couple of potential deadlock situations:
		// 1. Closing the broker on this go-routine deadlocks because close waits until this callback completes (which
		//    of course it cannot do, because it is blocked on Close())
		// 2. Calling close in the go-routine running eventLoop() also deadlocks because
		//    it cannot drain the produceCompleted channel if it is blocked in Close() and close cannot complete because
		//    it cannot add anything into the produceCompleted channel (as the go-routine in despatchInput is blocked)
		// TODO: would the Close code be cleaner if it was moved into sendToBroker()?
		go func() {
			fmt.Println("closing broker")
			if !ap.mutedBrokers.contains(brokerID) {
				// Guard against broker already been muted, as in the case of multiple inflight batches,
				// it's possible for one batch to fail and mute the broker, and then a second to fail and
				// also try to mute the broker
				// TODO: this avoids the panic, but is it the desired behavior?
				ap.mutedBrokers.add(brokerID, retryDuration)
			}

			b, berr := ap.client.Broker(brokerID)
			if berr == nil {
				b.Close()
			}

			ap.produceCompleted <- &asyncProduceResult{
				brokerID: brokerID,
				batch:    batch,
				response: response,
				err:      err,
			}
		}()
	} else {
		ap.produceCompleted <- &asyncProduceResult{
			brokerID: brokerID,
			batch:    batch,
			response: response,
			err:      err,
		}
	}
}

type asyncProduceResult struct {
	brokerID int32
	batch    *batch
	response *ProduceResponse
	err      error
}

func (ap *asyncProducer2) addNewPartitionsToTransaction() error {
	return retry(retryConfig{
		maxRetries:     ap.config.Producer.Transaction.Retry.Max,
		backoffFunc:    ap.config.Producer.Transaction.Retry.BackoffFunc,
		defaultBackoff: ap.config.Producer.Transaction.Retry.Backoff,
	}, func() error {
		if len(ap.txNewPartitions) == 0 {
			return nil // No new partitions to add - success!
		}

		coordinator, err := ap.client.TransactionCoordinator(ap.txID)
		if err != nil {
			return retryError(err)
		}

		tps := map[string][]int32{}
		for tp := range ap.txNewPartitions {
			tps[tp.topic] = append(tps[tp.topic], tp.partition)
		}
		req := &AddPartitionsToTxnRequest{
			TransactionalID: ap.txID,
			ProducerID:      ap.producerID,
			ProducerEpoch:   ap.producerEpoch,
			TopicPartitions: tps,
		}
		if ap.config.Version.IsAtLeast(V2_7_0_0) {
			// Version 2 adds the support for new error code PRODUCER_FENCED.
			req.Version = 2
		} else if ap.config.Version.IsAtLeast(V2_0_0_0) {
			// Version 1 is the same as version 0.
			req.Version = 1
		}

		resp, err := coordinator.AddPartitionsToTxn(req)
		if err != nil {
			// Likely a network interruption. Try to re-establish connectivity to the coordinator.
			_ = coordinator.Close()
			_ = ap.client.RefreshTransactionCoordinator(ap.txID)
			return retryError(err)
		}

		var retry error

		// Response can contain different errors for different partitions. For each partition, decide if:
		// 1. The operation succeeded. The partition can be removed from the set of partitions to add,
		//    and added to the set of partitions in the transaction. No retry of the add API is required.
		// 2. The operation failed but should be retried. These partitions are left in the set of partitions
		//    to be added and the add API is retried.
		// 3. The operation failed and the transaction should be aborted. All the pending partitions to be
		//    added are discarded, and the transaction is marked as abort-only. No retry of the add API is
		//    attempted.
		for topicName, partitionErrors := range resp.Errors {
			for _, pe := range partitionErrors {
				switch {
				case pe.Err == ErrNoError: // Success
					tp := topicPartition{topicName, pe.Partition}
					delete(ap.txNewPartitions, tp)
					ap.txAddedPartitions[tp] = struct{}{}
				case pe.Err == ErrConsumerCoordinatorNotAvailable || pe.Err == ErrNotCoordinatorForConsumer:
					// Refresh the coordinator and try again.
					_ = coordinator.Close()
					_ = ap.client.RefreshTransactionCoordinator(ap.txID)
					retry = retryError(pe.Err)
				case pe.Err == ErrConcurrentTransactions:
					// See:  https://issues.apache.org/jira/browse/KAFKA-5482
					retry = retryError(pe.Err)
					if len(ap.txAddedPartitions) == 0 {
						retry = retryError(pe.Err).withBackoff(time.Millisecond * 20)
					}
				case isRetryableError(pe.Err):
					retry = retryError(pe.Err)
				case pe.Err == ErrInvalidProducerEpoch || pe.Err == ErrProducerFenced:
					ap.fatalError(ErrProducerFenced)
					ap.txNewPartitions = map[topicPartition]struct{}{}
				case pe.Err == ErrTransactionalIDAuthorizationFailed ||
					pe.Err == ErrInvalidTxnState || pe.Err == ErrInvalidProducerIDMapping:
					ap.fatalError(ErrProducerFenced)
					ap.txNewPartitions = map[topicPartition]struct{}{}
				case pe.Err == ErrTopicAuthorizationFailed || pe.Err == ErrOperationNotAttempted:
					ap.abortableError(pe.Err)
					ap.txNewPartitions = map[topicPartition]struct{}{}
				case pe.Err == ErrUnknownProducerID:
					// TODO: the Java code has can optionally treat this as a non-fatal
					// error, depending on the protocol version being used. We're going
					// to start off by assuming it's always fatal.
					ap.fatalError(pe.Err)
				default:
					// TODO: is there a better error code if we get here?
					ap.abortableError(errors.New("unexpected error"))
				}
			}
		}

		// TODO: this assumes that txAbortErr is still set if the error was a
		// fatal error - need to firm up a decision on this.
		if ap.txAbortErr != nil {
			return ap.txAbortErr
		} else if retry != nil {
			return retry
		}
		return nil
	})
}

// isRetryableError returns true if the error is listed as retry-able in the
// Kafka protocol documentation.
func isRetryableError(err KError) bool {
	switch err {
	case ErrInvalidMessage,
		ErrUnknownTopicOrPartition,
		ErrLeaderNotAvailable,
		ErrNotLeaderForPartition,
		ErrRequestTimedOut,
		ErrReplicaNotAvailable,
		ErrNetworkException,
		ErrOffsetsLoadInProgress,
		ErrNotCoordinatorForConsumer,
		ErrNotEnoughReplicas,
		ErrNotEnoughReplicasAfterAppend,
		ErrNotController,
		ErrConcurrentTransactions,
		ErrKafkaStorageError,
		ErrFetchSessionIDNotFound,
		ErrInvalidFetchSessionEpoch,
		ErrListenerNotFound,
		ErrFencedLeaderEpoch,
		ErrUnknownLeaderEpoch,
		ErrOffsetNotAvailable,
		ErrPreferredLeaderNotAvailable,
		ErrEligibleLeadersNotAvailable,
		ErrElectionNotNeeded,
		ErrUnstableOffsetCommit,
		ErrThrottlingQuotaExceeded:
		return true
	default:
		return false
	}
}

func (ap *asyncProducer2) maybeProduceBatches() {
	// Try to assign partitions to any futures awaiting partitioning, and find leaders for any awaiting a leader
	ap.processAwaitingPartitioning()
	ap.processAwaitingLeader()

	if ap.txID != "" {
		// If the transaction is abort-only then fail any messages held by the accumulator.
		if ap.txAbortErr != nil {
			ap.accumulator.failAll(ap.txAbortErr)
			return
		}

		// If new topic/partitions have been used with the current transaction, then send a
		// AddPartitionsToTxnRequest to the transaction coordinator.
		if err := ap.addNewPartitionsToTransaction(); err != nil {
			ap.txAbortErr = err
			fmt.Printf("ABORTING ALL WITH: %v\n", err.Error())
			ap.accumulator.failAll(ap.txAbortErr)
			return
		}
	}

	// TODO: tickers will panic if passed a zero duration - is that ever a valid configuration for Sarama?
	if ap.accumulator.hasIncompleteBatches() && !ap.batchTickerRunning {
		ap.batchTicker.Reset(100 * time.Millisecond) // TODO: get this from config
		ap.batchTickerRunning = true                 // TODO: maybe wrap this and the ticker into a struct to make tracking this easier...
	} else if !ap.accumulator.hasIncompleteBatches() && ap.batchTickerRunning {
		ap.batchTicker.Stop()
		ap.batchTickerRunning = false
	}

	// See if there is capacity to move accumulated batches into inflight.
	for _, brokerID := range ap.accumulator.brokerIDs() {
		if ap.mutedBrokers.contains(brokerID) {
			continue
		}

		inflightForBroker, ok := ap.inflight[brokerID]
		if !ok {
			inflightForBroker = &inflightInfo{
				batches: newBatchDeque(),
				muted:   false,
			}
			ap.inflight[brokerID] = inflightForBroker
		}
		fmt.Printf("inflights: %d %d\n", inflightForBroker.batches.size(), ap.config.Net.MaxOpenRequests)
		for inflightForBroker.batches.size() < ap.config.Net.MaxOpenRequests {
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
	fmt.Printf("updateInflightStatus batch %p marked resolved\n", produceResult.batch)
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
	fmt.Println("completeInFlightBatches called")
	for brokerID, brokerInFlight := range ap.inflight {
		// Start at the oldest in-flight batch, skipping any muted brokers:
		// - Remove resolved batches if they were successful
		// - Mute the broker if a resolved batch has failed (but don't remove it)
		// - Stop if an unresolved batch is encountered
		for !brokerInFlight.batches.isEmpty() && !brokerInFlight.muted {
			headBatch := brokerInFlight.batches.peek()
			fmt.Printf("completeInFlightBatches headBatch(%p).resolved=%t\n", headBatch, headBatch.resolved)
			if headBatch.resolved {
				if headBatch.hasFailures {
					brokerInFlight.muted = true
					break
				} else {
					headBatch.updateLastSuccessfulSequenceNumber(ap.lastSuccessfulSeqNum)
					if ap.txAbortErr == nil {
						headBatch.processSuccesses()
					} else {
						// TODO: simply this code for failing all messages in the batch.
						headBatch.failAll(ap.txAbortErr)
						for tp := range headBatch.futures {
							headBatch.processFailures(tp)
						}
					}
					fmt.Println("completeInFlightBatches removeFirst called")
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
		brokerInFlight.batches = newBatchDeque()
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

func (ap *asyncProducer2) completeFailedBatches(brokerID int32, inflight *batchDeque) {
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
			if !isRetryable(isFirst, err) || ap.txAbortErr != nil {
				if ap.txID != "" && ap.txAbortErr == nil {
					// Track if a non-retry-able error occurs in the scope of a transaction, as
					// this means the transaction can only be aborted.
					ap.txAbortErr = err
				}
				batch.processFailures(tp)
			}
		}
		isFirst = false
	}

	// Iterate over the inflight batches (starting at the newest), and re-queue any non-empty batches back
	// into the accumulator (empty batches would correspond to those that either succeeded for all topic partitions,
	// or failed with a non-retry-able error for all topic partitions)
	// TODO: update this comment to reflect the stuff that is happening if the transaction is aborting...
	for idx := inflight.size() - 1; idx >= 0; idx-- {
		batch := inflight.get(idx)
		if len(batch.futures) > 0 {
			if ap.txAbortErr != nil {
				batch.failAll(ap.txAbortErr)
				for tp := range batch.futures {
					batch.processFailures(tp)
				}
			} else {
				batch.resetErrors()
				batch.resolved = false // TODO: this should be done in a more general "reset" method (e.g. re-purpose batch.resetErrors())
				ap.accumulator.requeue(brokerID, batch)
			}
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
		// TODO: is this strictly true? It seems like they can also occur if the client sends a leaderEpoch
		// lower than the leader's epoch, which could (although it would be unlikely) occur if the leader was
		// re-elected to the same broker (which doesn't have to be after the first batch)
		return !isFirstFailingInflight
	}
	// Based on the retry-able errors documented in the Kafka Java client's ProduceResponse.java
	return errors.Is(kerr, ErrInvalidMessage) || // ErrInvalidMessage is called CORRUPT_MESSAGE in Java
		errors.Is(kerr, ErrUnknownTopicOrPartition) ||
		errors.Is(kerr, ErrNotLeaderForPartition) ||
		errors.Is(kerr, ErrNotEnoughReplicas) ||
		errors.Is(kerr, ErrNotEnoughReplicasAfterAppend)
}

// called by the go-routine that runs the event loop when it has been signalled to start a
// transaction.
func (ap *asyncProducer2) beginTransaction() error {
	switch ap.txState {
	case txStateUninitialized:
		// First transaction for this producer, need to call InitProducerID() on the coordinator.
		coordinator, err := ap.client.TransactionCoordinator(ap.txID)
		if err != nil {
			return err
		}
		req := &InitProducerIDRequest{
			TransactionalID:    &ap.txID,
			TransactionTimeout: ap.config.Producer.Transaction.Timeout,
		}
		resp, err := coordinator.InitProducerID(req)
		if err != nil {
			// TODO: should retry this, at least in the case the connection is broker.
			return err
		}
		if resp.Err != ErrNoError {
			// TODO: should retry this - e.g. if the coordinator has changed.
			return resp.Err
		}
		// TODO: do we need to store any of the other fields from the response in asyncProducer2?
		ap.txState = txStateInEmptyTransaction
		ap.txProducerEpoch = resp.ProducerEpoch
		ap.producerID = resp.ProducerID
	case txStateInitialized:
		// InitProducerID() already called by previous transaction.
		ap.txState = txStateInEmptyTransaction
	default:
		// TODO: is this the right error type?
		// TODO: error message could be more helpful!
		return errors.New("wrong state to begin transaction")
	}
	return nil
}

// called by the go-routine that runs the event loop when it has been signalled to end a
// transaction by committing or aborting. This process is asynchronous, as further runs of
// the event loop may be required to flush through any buffered records. So successful
// completion of this method leaves the transaction in either "committing" or "aborting"
// state. When there are no buffered messages, the event loop completes the transaction
// by sending the end transaction API flow to the broker.
func (ap *asyncProducer2) startCompletingTransaction(commit bool, errCh chan error) {
	switch ap.txState {
	case txStateInEmptyTransaction:
		panicIf(ap.txAbortErr != nil, "txAbortErr shouldn't be set if transaction is empty")
		// Committing / aborting an empty transaction has no effect.
		ap.txState = txStateInitialized

	case txStateInTransaction:
		if ap.txAbortErr != nil && commit {
			// If txAbortErr is set (because a non-retry-able error occurred in the scope of
			// the transaction) then the transaction cannot be committed. It must be aborted.
			errCh <- ap.txAbortErr
			return
		}
		panicIf(ap.flushTx != nil, "flushTx already set to a value")
		if commit {
			ap.txState = txStateCommittingTransaction
		} else {
			ap.txState = txStateAbortingTransaction
		}
		ap.flushTx = errCh

	default:
		// TODO: is this the right type of error?
		// TODO: message could be more helpful.
		if commit {
			errCh <- errors.New("transaction not in correct state to commit")
		} else {
			errCh <- errors.New("transaction not in correct state to abort")
		}
	}
}

// addOffsetToTxn is called by the eventLoop go-routine in response to a request to add
// offsets to a transaction.
// TODO: Each call performs two request/response exchanges with the broker is inefficient,
// as (unlike committing or aborting the transaction) it would be possible for the client
// to continue to send messages or add more offsets while these requests are in-flight.
// However, to implement this, Broker would need to have a way to send the add offset
// and offset commit requests and be notified of the responses later (much like the
// existing Broker.AsyncProduce(...) method already provides for sending messages).
func (ap *asyncProducer2) addOffsetsToTxn(offsets map[string][]*PartitionOffsetMetadata, groupId string) error {
	switch ap.txState {
	case txStateInEmptyTransaction:
		panicIf(ap.txAbortErr != nil, "shouldn't have txAbortErr set if transaction is empty")
		ap.txState = txStateInTransaction
	case txStateInTransaction:
		if ap.txAbortErr != nil {
			return ap.txAbortErr
		}
		// Otherwise drop of out of this switch statement and try to add the offset
	default:
		return errors.New("transaction not in correct state to add offsets")
	}

	txCoordinator, err := ap.client.TransactionCoordinator(ap.txID)
	if err != nil {
		// TODO: retry on this.
		ap.txAbortErr = err
		return err
	}
	req := &AddOffsetsToTxnRequest{
		TransactionalID: ap.txID,
		ProducerID:      ap.producerID,
		ProducerEpoch:   ap.producerEpoch,
		GroupID:         groupId,
	}
	resp, err := txCoordinator.AddOffsetsToTxn(req)
	if err != nil {
		// TODO: should retry this, at least in the case the connection is broker.
		ap.txAbortErr = err
		return err
	}
	if resp.Err != ErrNoError {
		// TODO: should retry this, as coordinator could have changed.
		ap.txAbortErr = resp.Err
		return resp.Err
	}

	groupCoordinator, err := ap.client.Coordinator(groupId)
	if err != nil {
		// TODO: retry on this
		ap.txAbortErr = err
		return err
	}
	commitReq := &TxnOffsetCommitRequest{
		Version:         2, // TODO: set this based on configured version...
		TransactionalID: ap.txID,
		ProducerEpoch:   ap.producerEpoch,
		ProducerID:      ap.producerID,
		GroupID:         groupId,
		Topics:          offsets,
	}
	commitResp, err := groupCoordinator.TxnOffsetCommit(commitReq)
	if err != nil {
		// TODO: add some retry logic
		ap.txAbortErr = err
		return err
	}
	// TODO: can commitResp be nil? The existing transaction manager code checks for this...

	for _, pes := range commitResp.Topics {
		for _, pe := range pes {
			if pe.Err != ErrNoError {
				ap.txAbortErr = pe.Err
				return pe.Err
			}
		}
	}
	return nil

}

// eventLoop...
func (ap *asyncProducer2) eventLoop() {
	for {
		select {
		case msg, ok := <-ap.input:
			DebugLogger.Println("read from input")
			if !ok {
				// TODO: this is probably not the right code path for being closed...
				DebugLogger.Println("closed")
				return
			}

			switch msg.flags2 {
			case txFlagBegin:
				msg.txResult <- ap.beginTransaction()
			case txFlagCommit:
				ap.startCompletingTransaction(true, msg.txResult)
			case txFlagAbort:
				ap.startCompletingTransaction(false, msg.txResult)
			case txFlagAddOffsets:
				msg.txResult <- ap.addOffsetsToTxn(msg.txAddOffsets, msg.txAddOffsetsGroupId)
			case txFlagNotControlMessage:
				if ap.txID != "" && !(ap.txState == txStateInEmptyTransaction || ap.txState == txStateInTransaction) {
					ap.errors <- &ProducerError{
						Msg: msg,
						// TODO: error message isn't very helpful.
						// TODO: should this error have a particular type?
						Err: errors.New("unable to produce message as transactional producer is not in the correct state"),
					}
					break
				}
				if ap.txAbortErr != nil {
					// TODO: combine into above if statement.
					ap.errors <- &ProducerError{
						Msg: msg,
						Err: ap.txAbortErr,
					}
				}
				// x
				// If the transaction abort error is set - then toss the message at this point...
				if ap.txAbortErr != nil {
					ap.errors <- &ProducerError{
						Msg: msg,
						Err: ap.txAbortErr,
					}
					break
				}

				// a new message has been passed to the async producer via its input channel
				// create a future for it
				f := ap.wrapIntoFuture(msg)

				// Add the future to those awaiting partitioning
				if pq, ok := ap.awaitingPartitioning[msg.Topic]; !ok {
					ap.awaitingPartitioning[msg.Topic] = newFutureDeque(f)
				} else {
					pq.add(f)
				}

				if ap.txID != "" && ap.txState == txStateInEmptyTransaction {
					// Accepted a message - current transaction is no longer empty.
					ap.txState = txStateInTransaction
				}
			}

		case <-ap.metadataRefreshed:
			DebugLogger.Println("metadata refresh")
			// metadata has been refreshed - see if this allows for more batches to be ready
			// to produce

		case <-ap.unMuteTimer.eventChannel():
			DebugLogger.Println("un-mute timer")
			// TODO: presumably we can just drop through for this?

		case <-ap.batchTicker.C:
			DebugLogger.Println("batch ticker")

			// deadline for time based batch completion has been reached - see if this has
			// caused more batches to become ready to produce.

		case produceResult := <-ap.produceCompleted:
			DebugLogger.Println("produce completed")

			updateInflightStatus(produceResult)
			ap.completeInFlightBatches()
		}

		ap.maybeProduceBatches()
		ap.maybeCompleteTransaction()
	}
}

// TODO: add support for retrying before returning an error.
func (ap *asyncProducer2) endTxn(commit bool) error {
	coordinator, err := ap.client.TransactionCoordinator(ap.txID)
	if err != nil {
		return err
	}
	req := &EndTxnRequest{
		TransactionalID:   ap.txID,
		ProducerID:        ap.producerID,
		ProducerEpoch:     ap.txProducerEpoch,
		TransactionResult: commit,
	}
	resp, err := coordinator.EndTxn(req)
	if err != nil {
		return err
	}
	if resp.Err != ErrNoError {
		return resp.Err
	}
	return nil
}

func (ap *asyncProducer2) maybeCompleteTransaction() {
	if ap.txID != "" && ap.isEmpty() && ap.flushTx != nil {
		if ap.txState == txStateCommittingTransaction {
			if ap.txAbortErr != nil {
				// Encountered a non-retry-able error during the commit.
				ap.txState = txStateInTransaction
				ap.flushTx <- ap.txAbortErr // TODO: could wrap the "send result and set to nil" into a function.
				ap.flushTx = nil
				return
			}
			if err := ap.endTxn(true); err != nil {
				ap.txAbortErr = err
				ap.txState = txStateInTransaction
				ap.flushTx <- ap.txAbortErr
				ap.flushTx = nil
				return
			}
		} else if ap.txState == txStateAbortingTransaction {
			if err := ap.endTxn(false); err != nil {
				if ap.txAbortErr == nil {
					ap.txAbortErr = err
				}
				ap.txState = txStateInTransaction
				ap.flushTx <- err
				ap.flushTx = nil
				return
			}
		}
		ap.txState = txStateInitialized
		ap.flushTx <- nil
		ap.flushTx = nil
		ap.txNewPartitions = map[topicPartition]struct{}{} // TODO: some of this stuff should live in a "resetTxState" method.
		ap.txAddedPartitions = map[topicPartition]struct{}{}
		// TODO:
		ap.txAbortErr = nil
		//x
		// TODO: if the transactional producer hits an un-recoverable error then the easiest way to
		// handle this would be to leave txAbortErr set to the error value - and maybe transition to
		// a new state (so we don't allow a further call to abort() to reset it).
	}
}

// partitionMessage attempts to determine which partition of a topic the message
// should be sent to, and if successful updates the partition field of the message.
// If unsuccessful, in addition to returning an error, the boolean result is set to
// true if the operation is retry-able.
func (ap *asyncProducer2) partitionMessage(msg *ProducerMessage) (bool, error) {
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
		return true, err
	}

	numPartitions := int32(len(partitions))
	if numPartitions == 0 {
		return true, ErrLeaderNotAvailable
	}

	choice, err := partitioner.Partition(msg, numPartitions)
	if err != nil {
		return true, err // retry-able, as the partitioner is plug-able so unclear if this can be transitory
	} else if choice < 0 || choice >= numPartitions {
		return false, ErrInvalidPartition // not retry-able as partitioner result was not valid
	}

	msg.Partition = partitions[choice]

	return false, nil
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

	// TODO: this looks suspicious... what happens if the leader has changed, surely we should
	// strip the corresponding partitions from the partial batch too?
	cb, ok := ba.currentBatches[brokerID]
	if !ok { // TODO: can this pattern be abstracted into a function that uses generics?
		cb = &partialBatch{
			batch: newBatch(),
		}
		ba.currentBatches[brokerID] = cb
	}

	if !cb.add(ba.config, future) {
		// Adding the future to the current batch would overflow
		readyBatch := ba.currentBatches[brokerID].batch
		ba.addReadyBatch(brokerID, leaderEpoch, readyBatch)
		cb = &partialBatch{
			batch: newBatch(),
		}
		ba.currentBatches[brokerID] = cb
	}

	if readyBatch := cb.ready(ba.config); readyBatch != nil {
		// Batch is ready for transmission - add it to the ready batches, and reset the current batch for this broker.
		ba.addReadyBatch(brokerID, leaderEpoch, readyBatch)
		delete(ba.currentBatches, brokerID)
	}
}

func (ba *batchAccumulator) addReadyBatch(brokerID int32, leaderEpoch int32, b *batch) {
	b.leaderEpoch = leaderEpoch
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
		DebugLogger.Println("batchAccumulator: poll returned nil")
		return nil
	}
	b := ready.removeFirst()
	return b
}

func (ba *batchAccumulator) hasIncompleteBatches() bool {
	// TODO: this is used to decide when to start/stop the ticker for time-based batch creation.
	// If we already have available batches, does it make sense to use time-based batch creation?
	return len(ba.currentBatches) != 0
}

func (ba *batchAccumulator) isEmpty() bool {
	if ba.hasIncompleteBatches() {
		return false
	}
	for _, ready := range ba.readyBatches {
		if !ready.isEmpty() {
			return false
		}
	}
	return true
}

func (ba *batchAccumulator) requeue(brokerID int32, b *batch) {
	ready, ok := ba.readyBatches[brokerID]
	if !ok {
		ready = newBatchDeque()
		ba.readyBatches[brokerID] = ready
	}
	ready.addFirst(b)
}

// failAll is used to fail all the messages held by the batch accumulator when a transaction
// becomes abort-only.
// TODO: re-factor the types this uses to reduce the amount of nesting / iteration that needs to take place here.
func (ba *batchAccumulator) failAll(err error) {
	for _, partial := range ba.currentBatches {
		if partial.batch != nil { // TODO: is this nil check required?
			for _, fd := range partial.batch.futures {
				for !fd.isEmpty() {
					f := fd.removeFirst()
					f.complete(err)
				}
			}
		}
	}
	for _, bd := range ba.readyBatches {
		for !bd.isEmpty() {
			batch := bd.removeFirst()
			batch.failAll(err)
			for tp := range batch.futures {
				batch.processFailures(tp)
			}
		}
	}
	ba.currentBatches = make(map[int32]*partialBatch)
	ba.readyBatches = make(map[int32]*batchDeque)
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

	isFirst := false
	if cb.sizeMsgs == 0 {
		isFirst = true
		cb.created = time.Now()
		cb.batch = newBatch()                                                  // TODO: check we're not re-initializing this...
		panicIf(msgBytes > int(MaxRequestSize-(10*1024)), "message too large") // TODO: reject messages that are too large before this point...
	}

	// TODO: these tests don't seem to include the current size of the partial batch. That should be added to msgBytes??

	if !isFirst {
		// TODO: do these tests need to include a condition for approaching maximum message size?

		if config.Producer.Flush.Bytes > 0 && msgBytes > config.Producer.Flush.Bytes {
			return false
		}
		if config.Producer.Flush.MaxMessages > 0 && cb.sizeMsgs == config.Producer.Flush.MaxMessages {
			return false
		}
		if time.Now().After(cb.created.Add(config.Producer.Flush.Frequency)) {
			// TODO: might be better to store a deadline in the batch, rather than re-calculate it each time
			return false
		}
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
	return d.elements[0]
}

func (d *deque[T, Q]) removeFirst() T {
	v := d.peek()
	d.elements = d.elements[1:]
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
	firstSequenceNum     map[topicPartition]int32
	leaderEpoch          int32
}

func newBatch() *batch {
	return &batch{
		futures:              nil,
		topicPartitionErrors: map[topicPartition]error{},
		firstSequenceNum:     map[topicPartition]int32{},
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

func (b *batch) updateLastSuccessfulSequenceNumber(lastSuccessfulSeqNum map[topicPartition]int32) {
	for tp, fdq := range b.futures {
		if b.topicPartitionErrors[tp] != nil {
			continue
		}
		lastSuccess := (b.firstSequenceNum[tp] + int32(fdq.size())) - 1 // TODO: -1 feels like a hack!
		fmt.Printf("update last successful sequence number: %v %v %d\n", tp, lastSuccessfulSeqNum[tp], lastSuccess)

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
	delete(b.topicPartitionErrors, tp)
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

func (b *batch) produceRequest(config *Config, producerID int64, producerEpoch int16, sequence map[topicPartition]int32) *ProduceRequest {
	// TODO: for the moment, we only care about building the version 2 batch format.
	pr := &ProduceRequest{
		RequiredAcks: config.Producer.RequiredAcks,
		Timeout:      int32(config.Producer.Timeout / time.Millisecond),
		records:      map[string]map[int32]Records{},
	}
	// TODO: mixing and matching config.Producer.Transaction.ID and ap.txID
	if config.Producer.Transaction.ID != "" {
		pr.TransactionalID = &config.Producer.Transaction.ID
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
			PartitionLeaderEpoch: b.leaderEpoch, // TODO: this is never set by Sarama...
			ProducerID:           producerID,
			ProducerEpoch:        producerEpoch,
			Codec:                config.Producer.Compression,
			CompressionLevel:     config.Producer.CompressionLevel,
			// TODO: presumably there are other important fields in here...
			Records: recordSlice,
			// FirstOffset - apparently this always needs to be zero??
			// FirstSequence - set below.
			IsTransactional: config.Producer.Transaction.ID != "",
		}
		if config.Producer.Idempotent {
			rb.FirstSequence = sequence[tp]
			sequence[tp] += int32(len(rb.Records))
		}
		records := newDefaultRecords(rb)
		partitionToRecords, ok := pr.records[tp.topic]
		if !ok {
			partitionToRecords = map[int32]Records{}
			pr.records[tp.topic] = partitionToRecords
		}
		partitionToRecords[tp.partition] = records
	}

	return pr
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
type (
	timer[T any] struct {
		in           chan timerEvent[T]
		out          chan T
		pending      *eventHeap[T]
		timer        *time.Timer
		nextDeadline *time.Time
	}

	timerEvent[T any] struct {
		event T
		time  time.Time
	}

	eventHeap[T any] []timerEvent[T]
)

func (eh eventHeap[T]) Len() int           { return len(eh) }
func (eh eventHeap[T]) Less(i, j int) bool { return eh[i].time.Before(eh[j].time) }
func (eh eventHeap[T]) Swap(i, j int)      { eh[i], eh[j] = eh[j], eh[i] }
func (eh *eventHeap[T]) Push(x any)        { *eh = append(*eh, x.(timerEvent[T])) }
func (eh *eventHeap[T]) Pop() any {
	old := *eh
	n := len(old)
	x := old[n-1]
	*eh = old[0 : n-1]
	return x
}

func newTimer[T any]() *timer[T] {
	t := &timer[T]{
		in:      make(chan timerEvent[T]),
		out:     make(chan T),
		pending: &eventHeap[T]{},
		timer:   time.NewTimer(time.Hour),
	}
	heap.Init(t.pending)
	t.timer.Stop()
	go t.run()
	return t
}

func (t *timer[T]) notifyIn(d time.Duration, event T) {
	t.in <- timerEvent[T]{
		event: event,
		time:  time.Now().Add(d),
	}
}

func (t *timer[T]) run() {
	select {
	case te, ok := <-t.in: // notifyIn called to add a new timer
		if !ok { // closed
			t.timer.Stop()
			return
		}
		heap.Push(t.pending, te)
		if t.nextDeadline == nil || te.time.Before(*t.nextDeadline) {
			// the new timer is the only timer / fires before any other timer
			t.nextDeadline = &te.time
			d := time.Until(te.time)
			if d < 1 {
				d = 1
			}
			t.timer.Stop()
			t.timer.Reset(d)
		}

	case <-t.timer.C: // a timer has fired
		t.nextDeadline = nil
		now := time.Now()
		for t.pending.Len() > 0 {
			te := heap.Pop(t.pending).(timerEvent[T])
			if te.time.Before(now) {
				t.out <- te.event
			} else {
				t.nextDeadline = &te.time
				heap.Push(t.pending, te)
				t.timer.Reset(te.time.Sub(now))
				break
			}
		}
	}
}

func (t *timer[T]) close() {
	close(t.in)
}

func (t *timer[T]) eventChannel() <-chan T {
	return t.out
}

// ================================================================================
type mutedSet[T comparable] struct {
	muted map[T]time.Time
	timer *timer[int]
}

func newMutedSet[T comparable](timer *timer[int]) *mutedSet[T] {
	return &mutedSet[T]{
		muted: map[T]time.Time{},
		timer: timer,
	}
}

func (ms *mutedSet[T]) contains(v T) bool {
	t, ok := ms.muted[v]
	if !ok {
		return false
	}
	if t.Before(time.Now()) {
		delete(ms.muted, v)
		return false
	}
	return true
}

func (ms *mutedSet[T]) add(v T, d time.Duration) {
	panicIf(ms.contains(v), "mutedSet already contains %v", v)
	fmt.Printf("muting %v for %d\n", v, d)
	ms.muted[v] = time.Now().Add(d)
	ms.timer.notifyIn(d, 0)
}

// ================================================================================
type txState string

const (
	// initial state for transactional producer, InitProducerID() has not been
	// called yet.
	txStateUninitialized = "uninitialized"

	// InitProducerID() has successfully being called.
	txStateInitialized = "initialized"

	// producer is in a state where it can accept messages / offsets into a transaction.
	// Currently no messages / offsets have been associated with the transaction.
	// TODO: should this be modeled as a separate state - or as a flag that we set?
	// TODO: wait until more code has been written, and hope it becomes clearer.
	txStateInEmptyTransaction = "inEmptyTransaction"

	// producer is in a state where it can accept messages / offsets into a transaction.
	// Currently there is at least one message or offset associated with the transaction.
	txStateInTransaction = "inTransaction"

	// commit has been called, and the transaction is in the process of being committed.
	// This state is required because committing the transaction occurs asynchronously
	// within the eventLoop (e.g. any in-flight messages will be flushed), so it is
	// possible for another go-routine to try and trigger a transaction state transition
	// while this is in-progress.
	txStateCommittingTransaction = "committingTransaction"

	// similar to 'txStateCommittingTransaction', but used to track that the process of
	// aborting the transaction has started.
	txStateAbortingTransaction = "abortingTransaction"
)

// ================================================================================
type txFlags string // txFlags used in ProducerMessage in async_producer.go
const (
	txFlagNotControlMessage = "" // Needs to be zero value for txFlags underlying type
	txFlagBegin             = "begin"
	txFlagCommit            = "commit"
	txFlagAbort             = "abort"
	txFlagAddOffsets        = "addOffsets"
)

// ================================================================================

func panicIf(b bool, msg string, v ...any) {
	if b {
		panic(fmt.Sprintf(msg, v...))
	}
}

// ================================================================================

type retryConfig struct {
	maxRetries     int
	backoffFunc    func(retries, maxRetries int) time.Duration
	defaultBackoff time.Duration
}

type retryableError struct {
	error
	backoff *time.Duration
}

func (re retryableError) Error() string {
	return re.error.Error()
}

func (re retryableError) Unwrap() error {
	return re.error
}

func (re retryableError) Is(err error) bool {
	_, ok := err.(retryableError)
	return ok
}

func (re retryableError) withBackoff(d time.Duration) retryableError {
	return retryableError{
		error:   re.error,
		backoff: &d,
	}
}

// retryError wraps an error into a retryableError
func retryError(err error) retryableError {
	return retryableError{
		error: err,
	}
}

// retry 'fn' based on the specified configuration.
//   - if `fn` returns nil then no (more) retries are attempted and nil is returned.
//   - if `fn` returns a retryableError then `fn` will be called again until the configured
//     retry limit is reached. If the retry limit is reached then the retryableError value
//     returned by `fn` will be unwrapped and the contained error will be returned.
//   - if `fn` returns an error that isn't wrapped into a retryableError then `fn` will
//     not be called again, and the error will be returned.
func retry(cfg retryConfig, fn func() error) error {
	retries := 0
	for {
		var re retryableError
		err := fn()
		if err == nil { // success
			return nil
		} else if errors.As(err, &re) { // retry
			retries++
			if retries > cfg.maxRetries {
				return re.error
			}
			if re.backoff != nil {
				time.Sleep(*re.backoff)
			} else if cfg.backoffFunc != nil {
				time.Sleep(cfg.backoffFunc(retries, cfg.maxRetries))
			} else {
				time.Sleep(cfg.defaultBackoff)
			}
		} else { // fail
			return err
		}
	}
}
