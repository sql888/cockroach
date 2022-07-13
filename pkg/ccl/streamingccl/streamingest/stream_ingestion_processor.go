// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

var minimumFlushInterval = settings.RegisterPublicDurationSettingWithExplicitUnit(
	settings.TenantWritable,
	"bulkio.stream_ingestion.minimum_flush_interval",
	"the minimum timestamp between flushes; flushes may still occur if internal buffers fill up",
	5*time.Second,
	nil, /* validateFn */
)

// checkForCutoverSignalFrequency is the frequency at which the resumer polls
// the system.jobs table to check whether the stream ingestion job has been
// signaled to cutover.
var cutoverSignalPollInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"bulkio.stream_ingestion.cutover_signal_poll_interval",
	"the interval at which the stream ingestion job checks if it has been signaled to cutover",
	30*time.Second,
	settings.NonNegativeDuration,
)

var streamIngestionResultTypes = []*types.T{
	types.Bytes, // jobspb.ResolvedSpans
}

type mvccKeyValues []storage.MVCCKeyValue

func (s mvccKeyValues) Len() int           { return len(s) }
func (s mvccKeyValues) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s mvccKeyValues) Less(i, j int) bool { return s[i].Key.Less(s[j].Key) }

type streamIngestionProcessor struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.StreamIngestionDataSpec
	output  execinfra.RowReceiver
	rekeyer *backupccl.KeyRewriter
	// rewriteToDiffKey Indicates whether we are rekeying a key into a different key.
	rewriteToDiffKey bool

	// curBatch temporarily batches MVCC Keys so they can be
	// sorted before ingestion.
	// TODO: This doesn't yet use a buffering adder since the current
	// implementation is specific to ingesting KV pairs without timestamps rather
	// than MVCCKeys.
	curBatch mvccKeyValues
	// batcher is used to flush SSTs to the storage layer.
	batcher           *bulk.SSTBatcher
	maxFlushRateTimer *timeutil.Timer

	// client is a streaming client which provides a stream of events from a given
	// address.
	forceClientForTests streamclient.Client
	// streamPartitionClients are a collection of streamclient.Client created for
	// consuming multiple partitions from a stream.
	streamPartitionClients []streamclient.Client

	// cutoverProvider indicates when the cutover time has been reached.
	cutoverProvider cutoverProvider

	// frontier keeps track of the progress for the spans tracked by this processor
	// and is used forward resolved spans
	frontier *span.Frontier
	// lastFlushTime keeps track of the last time that we flushed due to a
	// checkpoint timestamp event.
	lastFlushTime time.Time
	// When the event channel closes, we should flush any events that remains to
	// be buffered. The processor keeps track of if we're done seeing new events,
	// and have attempted to flush them with `internalDrained`.
	internalDrained bool

	// pollingWaitGroup registers the polling goroutine and waits for it to return
	// when the processor is being drained.
	pollingWaitGroup sync.WaitGroup

	// eventCh is the merged event channel of all of the partition event streams.
	eventCh chan partitionEvent

	// cutoverCh is used to convey that the ingestion job has been signaled to
	// cutover.
	cutoverCh chan struct{}

	// cg is used to receive the subscription of events from the source cluster.
	cg ctxgroup.Group

	// closePoller is used to shutdown the poller that checks the job for a
	// cutover signal.
	closePoller chan struct{}
	// cancelMergeAndWait cancels the merging goroutines and waits for them to
	// finish. It cannot be called concurrently with Next(), as it consumes from
	// the merged channel.
	cancelMergeAndWait func()

	// mu is used to provide thread-safe read-write operations to ingestionErr
	// and pollingErr.
	mu struct {
		syncutil.Mutex

		// ingestionErr stores any error that is returned from the worker goroutine so
		// that it can be forwarded through the DistSQL flow.
		ingestionErr error

		// pollingErr stores any error that is returned from the poller checking for a
		// cutover signal so that it can be forwarded through the DistSQL flow.
		pollingErr error
	}

	// metrics are monitoring all running ingestion jobs.
	metrics *Metrics
}

// partitionEvent augments a normal event with the partition it came from.
type partitionEvent struct {
	streamingccl.Event
	partition string
}

var (
	_ execinfra.Processor = &streamIngestionProcessor{}
	_ execinfra.RowSource = &streamIngestionProcessor{}
)

const streamIngestionProcessorName = "stream-ingestion-processor"

func newStreamIngestionDataProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.StreamIngestionDataSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	rekeyer, err := backupccl.MakeKeyRewriterFromRekeys(flowCtx.Codec(),
		nil /* tableRekeys */, []execinfrapb.TenantRekey{spec.TenantRekey},
		true /* restoreTenantFromStream */)
	if err != nil {
		return nil, err
	}
	trackedSpans := make([]roachpb.Span, 0)
	for _, partitionSpec := range spec.PartitionSpecs {
		trackedSpans = append(trackedSpans, partitionSpec.Spans...)
	}

	frontier, err := span.MakeFrontierAt(spec.StartTime, trackedSpans...)
	if err != nil {
		return nil, err
	}
	for _, resolvedSpan := range spec.Checkpoint.ResolvedSpans {
		if _, err := frontier.Forward(resolvedSpan.Span, resolvedSpan.Timestamp); err != nil {
			return nil, err
		}
	}

	sip := &streamIngestionProcessor{
		flowCtx:           flowCtx,
		spec:              spec,
		output:            output,
		curBatch:          make([]storage.MVCCKeyValue, 0),
		frontier:          frontier,
		maxFlushRateTimer: timeutil.NewTimer(),
		cutoverProvider: &cutoverFromJobProgress{
			jobID:    jobspb.JobID(spec.JobID),
			registry: flowCtx.Cfg.JobRegistry,
		},
		cutoverCh:        make(chan struct{}),
		closePoller:      make(chan struct{}),
		rekeyer:          rekeyer,
		rewriteToDiffKey: spec.TenantRekey.NewID != spec.TenantRekey.OldID,
	}
	if err := sip.Init(sip, post, streamIngestionResultTypes, flowCtx, processorID, output, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				sip.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	return sip, nil
}

// Start is part of the RowSource interface.
func (sip *streamIngestionProcessor) Start(ctx context.Context) {
	ctx = logtags.AddTag(ctx, "job", sip.spec.JobID)
	log.Infof(ctx, "starting ingest proc")
	ctx = sip.StartInternal(ctx, streamIngestionProcessorName)

	sip.metrics = sip.flowCtx.Cfg.JobRegistry.MetricsStruct().StreamIngest.(*Metrics)

	evalCtx := sip.FlowCtx.EvalCtx
	db := sip.FlowCtx.Cfg.DB
	var err error
	sip.batcher, err = bulk.MakeStreamSSTBatcher(ctx, db, evalCtx.Settings,
		sip.flowCtx.Cfg.BackupMonitor.MakeBoundAccount(), sip.flowCtx.Cfg.BulkSenderLimiter)
	if err != nil {
		sip.MoveToDraining(errors.Wrap(err, "creating stream sst batcher"))
		return
	}

	// Start a poller that checks if the stream ingestion job has been signaled to
	// cutover.
	sip.pollingWaitGroup.Add(1)
	go func() {
		defer sip.pollingWaitGroup.Done()
		err := sip.checkForCutoverSignal(ctx, sip.closePoller)
		if err != nil {
			sip.mu.Lock()
			sip.mu.pollingErr = errors.Wrap(err, "error while polling job for cutover signal")
			sip.mu.Unlock()
		}
	}()

	log.Infof(ctx, "starting %d stream partitions", len(sip.spec.PartitionSpecs))

	// Initialize the event streams.
	subscriptions := make(map[string]streamclient.Subscription)
	sip.cg = ctxgroup.WithContext(ctx)
	sip.streamPartitionClients = make([]streamclient.Client, 0)
	for _, partitionSpec := range sip.spec.PartitionSpecs {
		id := partitionSpec.PartitionID
		token := streamclient.SubscriptionToken(partitionSpec.SubscriptionToken)
		addr := partitionSpec.Address
		var streamClient streamclient.Client
		if sip.forceClientForTests != nil {
			streamClient = sip.forceClientForTests
			log.Infof(ctx, "using testing client")
		} else {
			streamClient, err = streamclient.NewStreamClient(ctx, streamingccl.StreamAddress(addr))
			if err != nil {
				sip.MoveToDraining(errors.Wrapf(err, "creating client for partition spec %q from %q", token, addr))
				return
			}
			sip.streamPartitionClients = append(sip.streamPartitionClients, streamClient)
		}

		startTime := frontierForSpans(sip.frontier, partitionSpec.Spans...)

		if streamingKnobs, ok := sip.FlowCtx.TestingKnobs().StreamingTestingKnobs.(*sql.StreamingTestingKnobs); ok {
			if streamingKnobs != nil && streamingKnobs.BeforeClientSubscribe != nil {
				streamingKnobs.BeforeClientSubscribe(string(token), startTime)
			}
		}

		sub, err := streamClient.Subscribe(ctx, streaming.StreamID(sip.spec.StreamID), token, startTime)

		if err != nil {
			sip.MoveToDraining(errors.Wrapf(err, "consuming partition %v", addr))
			return
		}
		subscriptions[id] = sub
		sip.cg.GoCtx(sub.Subscribe)
	}
	sip.eventCh = sip.merge(ctx, subscriptions)
}

// Next is part of the RowSource interface.
func (sip *streamIngestionProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if sip.State != execinfra.StateRunning {
		return nil, sip.DrainHelper()
	}

	sip.mu.Lock()
	err := sip.mu.pollingErr
	sip.mu.Unlock()
	if err != nil {
		sip.MoveToDraining(err)
		return nil, sip.DrainHelper()
	}

	progressUpdate, err := sip.consumeEvents()
	if err != nil {
		sip.MoveToDraining(err)
		return nil, sip.DrainHelper()
	}

	if progressUpdate != nil {
		progressBytes, err := protoutil.Marshal(progressUpdate)
		if err != nil {
			sip.MoveToDraining(err)
			return nil, sip.DrainHelper()
		}
		row := rowenc.EncDatumRow{
			rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(progressBytes))),
		}
		return row, nil
	}

	sip.mu.Lock()
	err = sip.mu.ingestionErr
	sip.mu.Unlock()
	if err != nil {
		sip.MoveToDraining(err)
		return nil, sip.DrainHelper()
	}

	sip.MoveToDraining(nil /* error */)
	return nil, sip.DrainHelper()
}

// MustBeStreaming implements the Processor interface.
func (sip *streamIngestionProcessor) MustBeStreaming() bool {
	return true
}

// ConsumerClosed is part of the RowSource interface.
func (sip *streamIngestionProcessor) ConsumerClosed() {
	sip.close()
}

func (sip *streamIngestionProcessor) close() {
	if sip.Closed {
		return
	}

	for _, client := range sip.streamPartitionClients {
		_ = client.Close(sip.Ctx)
	}
	if sip.batcher != nil {
		sip.batcher.Close(sip.Ctx)
	}
	if sip.maxFlushRateTimer != nil {
		sip.maxFlushRateTimer.Stop()
	}
	close(sip.closePoller)
	// Wait for the processor goroutine to return so that we do not access
	// processor state once it has shutdown.
	sip.pollingWaitGroup.Wait()
	// Wait for the merge goroutine.
	if sip.cancelMergeAndWait != nil {
		sip.cancelMergeAndWait()
	}

	sip.InternalClose()
}

// checkForCutoverSignal periodically loads the job progress to check for the
// sentinel value that signals the ingestion job to complete.
func (sip *streamIngestionProcessor) checkForCutoverSignal(
	ctx context.Context, stopPoller chan struct{},
) error {
	sv := &sip.flowCtx.Cfg.Settings.SV
	tick := time.NewTicker(cutoverSignalPollInterval.Get(sv))
	defer tick.Stop()
	for {
		select {
		case <-stopPoller:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			cutoverReached, err := sip.cutoverProvider.cutoverReached(ctx)
			if err != nil {
				return err
			}
			if cutoverReached {
				sip.cutoverCh <- struct{}{}
				return nil
			}
		}
	}
}

// merge takes events from all the streams and merges them into a single
// channel.
func (sip *streamIngestionProcessor) merge(
	ctx context.Context, subscriptions map[string]streamclient.Subscription,
) chan partitionEvent {
	merged := make(chan partitionEvent)

	ctx, cancel := context.WithCancel(ctx)
	g := ctxgroup.WithContext(ctx)

	sip.cancelMergeAndWait = func() {
		cancel()
		// Wait until the merged channel is closed by the goroutine above.
		for range merged {
		}
	}

	for partition, sub := range subscriptions {
		partition := partition
		sub := sub
		g.GoCtx(func(ctx context.Context) error {
			ctxDone := ctx.Done()
			for {
				select {
				case event, ok := <-sub.Events():
					if !ok {
						return sub.Err()
					}

					pe := partitionEvent{
						Event:     event,
						partition: partition,
					}

					select {
					case merged <- pe:
					case <-ctxDone:
						return ctx.Err()
					}
				case <-ctxDone:
					return ctx.Err()
				}
			}
		})
	}
	go func() {
		err := g.Wait()
		sip.mu.Lock()
		defer sip.mu.Unlock()
		sip.mu.ingestionErr = err
		close(merged)
	}()

	return merged
}

// consumeEvents handles processing events on the merged event queue and returns
// once a checkpoint event has been emitted so that it can inform the downstream
// frontier processor to consider updating the frontier.
//
// It should only make a claim that about the resolved timestamp of a partition
// increasing after it has flushed all KV events previously received by that
// partition.
func (sip *streamIngestionProcessor) consumeEvents() (*jobspb.ResolvedSpans, error) {
	// This timer is used to batch up resolved timestamp events that occur within
	// a given time interval, as to not flush too often and allow the buffer to
	// accumulate data.
	// A flush may still occur if the in memory buffer becomes full.
	sv := &sip.FlowCtx.Cfg.Settings.SV

	if sip.internalDrained {
		return nil, nil
	}

	for sip.State == execinfra.StateRunning {
		select {
		case event, ok := <-sip.eventCh:
			if !ok {
				sip.internalDrained = true
				return sip.flush()
			}
			if event.Type() == streamingccl.KVEvent {
				sip.metrics.AdmitLatency.RecordValue(
					timeutil.Since(event.GetKV().Value.Timestamp.GoTime()).Nanoseconds())
			}

			if streamingKnobs, ok := sip.FlowCtx.TestingKnobs().StreamingTestingKnobs.(*sql.StreamingTestingKnobs); ok {
				if streamingKnobs != nil && streamingKnobs.RunAfterReceivingEvent != nil {
					if err := streamingKnobs.RunAfterReceivingEvent(sip.Ctx); err != nil {
						return nil, err
					}
				}
			}

			switch event.Type() {
			case streamingccl.KVEvent:
				if err := sip.bufferKV(event.GetKV()); err != nil {
					return nil, err
				}
			case streamingccl.SSTableEvent:
				if err := sip.bufferSST(event.GetSSTable()); err != nil {
					return nil, err
				}
			case streamingccl.CheckpointEvent:
				if err := sip.bufferCheckpoint(event); err != nil {
					return nil, err
				}

				minFlushInterval := minimumFlushInterval.Get(sv)
				if timeutil.Since(sip.lastFlushTime) < minFlushInterval {
					// Not enough time has passed since the last flush. Let's set a timer
					// that will trigger a flush eventually.
					// TODO: This resets the timer every checkpoint event, but we only
					// need to reset it once.
					sip.maxFlushRateTimer.Reset(time.Until(sip.lastFlushTime.Add(minFlushInterval)))
					continue
				}

				return sip.flush()
			case streamingccl.GenerationEvent:
				log.Info(sip.Ctx, "GenerationEvent received")
				select {
				case <-sip.cutoverCh:
					sip.internalDrained = true
					return nil, nil
				case <-sip.Ctx.Done():
					return nil, sip.Ctx.Err()
				}
			default:
				return nil, errors.Newf("unknown streaming event type %v", event.Type())
			}
		case <-sip.cutoverCh:
			// TODO(adityamaru): Currently, the cutover time can only be <= resolved
			// ts written to the job progress and so there is no point flushing
			// buffered KVs only to be reverted. When we allow users to specify a
			// cutover ts in the future, this will need to change.
			//
			// On receiving a cutover signal, the processor must shutdown gracefully.
			sip.internalDrained = true
			return nil, nil

		case <-sip.maxFlushRateTimer.C:
			sip.maxFlushRateTimer.Read = true
			return sip.flush()
		}
	}

	// No longer running, we've closed our batcher.
	return nil, nil
}

func (sip *streamIngestionProcessor) bufferSST(sst *roachpb.RangeFeedSSTable) error {
	_, sp := tracing.ChildSpan(sip.Ctx, "stream-ingestion-buffer-sst")
	defer sp.Finish()

	iter, err := storage.NewMemSSTIterator(sst.Data, true)
	if err != nil {
		return err
	}
	defer iter.Close()
	for ; ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return err
		} else if !ok { // cursor passed the span end key
			break
		}
		if err = sip.bufferKV(&roachpb.KeyValue{
			Key: iter.UnsafeKey().Key,
			Value: roachpb.Value{
				RawBytes:  iter.UnsafeValue(),
				Timestamp: iter.UnsafeKey().Timestamp,
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

func (sip *streamIngestionProcessor) bufferKV(kv *roachpb.KeyValue) error {
	// TODO: In addition to flushing when receiving a checkpoint event, we
	// should also flush when we've buffered sufficient KVs. A buffering adder
	// would save us here.
	if kv == nil {
		return errors.New("kv event expected to have kv")
	}

	rekey, ok, err := sip.rekeyer.RewriteKey(kv.Key)
	if !ok {
		return errors.New("every key is expected to match tenant prefix")
	}
	if err != nil {
		return err
	}
	kv.Key = rekey

	if sip.rewriteToDiffKey {
		kv.Value.ClearChecksum()
		kv.Value.InitChecksum(kv.Key)
	}

	mvccKey := storage.MVCCKey{
		Key:       kv.Key,
		Timestamp: kv.Value.Timestamp,
	}
	sip.curBatch = append(sip.curBatch, storage.MVCCKeyValue{Key: mvccKey, Value: kv.Value.RawBytes})
	return nil
}

func (sip *streamIngestionProcessor) bufferCheckpoint(event partitionEvent) error {
	resolvedSpans := *event.GetResolvedSpans()
	if resolvedSpans == nil {
		return errors.New("checkpoint event expected to have resolved spans")
	}

	lowestTimestamp := hlc.MaxTimestamp
	highestTimestamp := hlc.MinTimestamp
	for _, resolvedSpan := range resolvedSpans {
		if resolvedSpan.Timestamp.Less(lowestTimestamp) {
			lowestTimestamp = resolvedSpan.Timestamp
		}
		if highestTimestamp.Less(resolvedSpan.Timestamp) {
			highestTimestamp = resolvedSpan.Timestamp
		}
		_, err := sip.frontier.Forward(resolvedSpan.Span, resolvedSpan.Timestamp)
		if err != nil {
			return errors.Wrap(err, "unable to forward checkpoint frontier")
		}
	}

	sip.metrics.EarliestDataCheckpointSpan.Update(lowestTimestamp.GoTime().UnixNano())
	sip.metrics.LatestDataCheckpointSpan.Update(highestTimestamp.GoTime().UnixNano())
	sip.metrics.DataCheckpointSpanCount.Update(int64(len(resolvedSpans)))
	sip.metrics.ResolvedEvents.Inc(1)
	return nil
}

func (sip *streamIngestionProcessor) flush() (*jobspb.ResolvedSpans, error) {
	ctx, sp := tracing.ChildSpan(sip.Ctx, "stream-ingestion-flush")
	defer sp.Finish()

	flushedCheckpoints := jobspb.ResolvedSpans{ResolvedSpans: make([]jobspb.ResolvedSpan, 0)}
	// Ensure that the current batch is sorted.
	sort.Sort(sip.curBatch)

	totalSize := 0
	minBatchMVCCTimestamp := hlc.MaxTimestamp
	for _, kv := range sip.curBatch {
		if err := sip.batcher.AddMVCCKey(ctx, kv.Key, kv.Value); err != nil {
			return nil, errors.Wrapf(err, "adding key %+v", kv)
		}
		if kv.Key.Timestamp.Less(minBatchMVCCTimestamp) {
			minBatchMVCCTimestamp = kv.Key.Timestamp
		}
		totalSize += len(kv.Key.Key) + len(kv.Value)
	}

	if len(sip.curBatch) > 0 {
		preFlushTime := timeutil.Now()
		defer func() {
			sip.metrics.FlushHistNanos.RecordValue(timeutil.Since(preFlushTime).Nanoseconds())
			sip.metrics.CommitLatency.RecordValue(timeutil.Since(minBatchMVCCTimestamp.GoTime()).Nanoseconds())
			sip.metrics.Flushes.Inc(1)
			sip.metrics.IngestedBytes.Inc(int64(totalSize))
			sip.metrics.IngestedEvents.Inc(int64(len(sip.curBatch)))
		}()
		if err := sip.batcher.Flush(ctx); err != nil {
			return nil, errors.Wrap(err, "flushing")
		}
	}

	// Go through buffered checkpoint events, and put them on the channel to be
	// emitted to the downstream frontier processor.
	sip.frontier.Entries(func(sp roachpb.Span, ts hlc.Timestamp) span.OpResult {
		flushedCheckpoints.ResolvedSpans = append(flushedCheckpoints.ResolvedSpans, jobspb.ResolvedSpan{Span: sp, Timestamp: ts})
		return span.ContinueMatch
	})

	// Reset the current batch.
	sip.curBatch = nil
	sip.lastFlushTime = timeutil.Now()

	return &flushedCheckpoints, sip.batcher.Reset(ctx)
}

// cutoverProvider allows us to override how we decide when the job has reached
// the cutover places in tests.
type cutoverProvider interface {
	cutoverReached(context.Context) (bool, error)
}

// custoverFromJobProgress is a cutoverProvider that decides whether the cutover
// time has been reached based on the progress stored on the job record.
type cutoverFromJobProgress struct {
	registry *jobs.Registry
	jobID    jobspb.JobID
}

func (c *cutoverFromJobProgress) cutoverReached(ctx context.Context) (bool, error) {
	j, err := c.registry.LoadJob(ctx, c.jobID)
	if err != nil {
		return false, err
	}
	progress := j.Progress()
	var sp *jobspb.Progress_StreamIngest
	var ok bool
	if sp, ok = progress.GetDetails().(*jobspb.Progress_StreamIngest); !ok {
		return false, errors.Newf("unknown progress type %T in stream ingestion job %d",
			j.Progress().Progress, c.jobID)
	}
	// Job has been signaled to complete.
	if resolvedTimestamp := progress.GetHighWater(); !sp.StreamIngest.CutoverTime.IsEmpty() &&
		resolvedTimestamp != nil && sp.StreamIngest.CutoverTime.Less(*resolvedTimestamp) {
		return true, nil
	}

	return false, nil
}

// frontierForSpan returns the lowest timestamp in the frontier within the given
// subspans.  If the subspans are entirely outside the Frontier's tracked span
// an empty timestamp is returned.
func frontierForSpans(f *span.Frontier, spans ...roachpb.Span) hlc.Timestamp {
	minTimestamp := hlc.Timestamp{}
	for _, spanToCheck := range spans {
		f.SpanEntries(spanToCheck, func(frontierSpan roachpb.Span, ts hlc.Timestamp) span.OpResult {
			if minTimestamp.IsEmpty() || ts.Less(minTimestamp) {
				minTimestamp = ts
			}
			return span.ContinueMatch
		})
	}
	return minTimestamp
}

func init() {
	rowexec.NewStreamIngestionDataProcessor = newStreamIngestionDataProcessor
}
