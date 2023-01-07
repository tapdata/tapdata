



package io.tapdata.partition;

import io.tapdata.async.master.*;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.schema.TapIndexEx;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.error.ConnectorErrors;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.source.CountByPartitionFilterFunction;
import io.tapdata.pdk.apis.functions.connector.source.QueryFieldMinMaxValueFunction;
import io.tapdata.pdk.apis.partition.FieldMinMaxValue;
import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;
import io.tapdata.pdk.apis.partition.splitter.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * @author aplomb
 */
public class DatabaseReadPartitionSplitter {
	private TypeSplitterMap typeSplitterMap;
	private String id;
	private ScheduledExecutorService handleReadPartitionScheduler;
//	private ScheduledFuture<?> partitionFuture;

	public DatabaseReadPartitionSplitter id(String id) {
		this.id = id;
		return this;
	}
	private long maxRecordWithRatioInPartition;
//	private ParallelWorker parallelWorker;
//	private QueueWorker readPartitionTimerTask;

	private final PartitionCollector rootPartitionCollector = new PartitionCollector();
//	private PartitionCollector currentPartitionCollector;

	public DatabaseReadPartitionSplitter() {
		id = "DatabaseReadPartitionSplitter_" + UUID.randomUUID();
	}

	public DatabaseReadPartitionSplitter typeSplitterMap(TypeSplitterMap typeSplitterMap) {
		this.typeSplitterMap = typeSplitterMap;
		return this;
	}

	private int splitPiecesForCountIsSlow = 200;
	public DatabaseReadPartitionSplitter splitPiecesForCountIsSlow(int splitPiecesForCountIsSlow) {
		this.splitPiecesForCountIsSlow = splitPiecesForCountIsSlow;
		return this;
	}
	private boolean countIsSlow;
	public DatabaseReadPartitionSplitter countIsSlow(boolean countIsSlow) {
		this.countIsSlow = countIsSlow;
		return this;
	}
	private TapConnectorContext context;
	public DatabaseReadPartitionSplitter context(TapConnectorContext connectorContext) {
		context = connectorContext;
		return this;
	}
	private TapTable table;
	public DatabaseReadPartitionSplitter table(TapTable table) {
		this.table = table;
		return this;
	}
	private long maxRecordInPartition = 500000;
	public DatabaseReadPartitionSplitter maxRecordInPartition(long maxRecordInPartition) {
		this.maxRecordInPartition = maxRecordInPartition;
		return this;
	}
	private int maxRecordRatio = 4;
	public DatabaseReadPartitionSplitter maxRecordRatio(int maxRecordRatio) {
		this.maxRecordRatio = maxRecordRatio;
		return this;
	}
	private int countNumOfThread = 3;
	public DatabaseReadPartitionSplitter countNumOfThread(int countNumOfThread) {
		this.countNumOfThread = countNumOfThread;
		return this;
	}
	private List<ReadPartition> existingPartitions;
	public DatabaseReadPartitionSplitter existingPartitions(List<ReadPartition> existingPartitions) {
		this.existingPartitions = existingPartitions;
		return this;
	}
	private Consumer<ReadPartition> consumer;
	public DatabaseReadPartitionSplitter consumer(Consumer<ReadPartition> consumer) {
		this.consumer = consumer;
		return this;
	}
	private SplitCompleteListener splitCompleteListener;
	public DatabaseReadPartitionSplitter splitCompleteListener(SplitCompleteListener splitCompleteListener) {
		this.splitCompleteListener = splitCompleteListener;
		return this;
	}
	private CountByPartitionFilterFunction countByPartitionFilter;
	public DatabaseReadPartitionSplitter countByPartitionFilter(CountByPartitionFilterFunction function) {
		countByPartitionFilter = function;
		return this;
	}
	private QueryFieldMinMaxValueFunction queryFieldMinMaxValue;
	public DatabaseReadPartitionSplitter queryFieldMinMaxValue(QueryFieldMinMaxValueFunction function) {
		queryFieldMinMaxValue = function;
		return this;
	}

	public void startSplitting() {
		if(countByPartitionFilter == null) {
			context.getLog().info("countByPartitionFilter function is not implemented, consider countIsSlow = true, will only use min/max for splitting.");
			countIsSlow = true;
		}
//			throw new CoreException(ConnectorErrors.MISSING_COUNT_BY_PARTITION_FILTER, "Missing countByPartitionFilter while startSplitting");
		if(queryFieldMinMaxValue == null)
			throw new CoreException(ConnectorErrors.MISSING_QUERY_FIELD_MIN_MAX_VALUE, "Missing queryFieldMinMaxValue while startSplitting");
		if(table == null)
			throw new CoreException(ConnectorErrors.MISSING_TABLE, "Missing table while startSplitting");
		if(consumer == null)
			throw new CoreException(ConnectorErrors.MISSING_CONSUMER, "Missing consumer while startSplitting");
		if(context == null)
			throw new CoreException(ConnectorErrors.MISSING_CONNECTOR_CONTEXT, "Missing connector context while startSplitting");

		context.getLog().info(id + ": Start splitting for table {}, maxRecordInPartition {}", table.getId(), maxRecordInPartition);
		TapPartitionFilter partitionFilter = TapPartitionFilter.create();
		long time = System.currentTimeMillis();
		long count = countIsSlow ? -1 : countByPartitionFilter.countByPartitionFilter(context, table, partitionFilter);
		context.getLog().info(id + ": Initial count {}, takes {} countIsSlow {}", count, (System.currentTimeMillis() - time), countIsSlow);

		TapIndexEx partitionIndex = table.partitionIndex();
		context.getLog().info(id + ": partitionIndex {}", partitionIndex);
		maxRecordWithRatioInPartition = maxRecordInPartition * maxRecordRatio;
		context.getLog().info(id + ": Record range for a partition is {} ~ {}", maxRecordInPartition, maxRecordWithRatioInPartition);
		final String prefix = "P_";
		int index = 0;
		if(partitionIndex == null || (count != -1 && maxRecordWithRatioInPartition > count)) {
			ReadPartition readPartition = ReadPartition.create().id(prefix + UUID.randomUUID().toString().replace("-", "")).partitionFilter(TapPartitionFilter.create());
			context.getLog().info(id + ": Count {} less than max {}, will not split, but only on ReadPartition {}", count, maxRecordWithRatioInPartition, readPartition);
			consumer.accept(readPartition);
			if(splitCompleteListener != null)
				splitCompleteListener.completed(id);
			context.getLog().info(id + ": Split job done only one piece here, no need to split, maxRecordWithRatioInPartition {} partitionIndex {}", maxRecordWithRatioInPartition, partitionIndex);
		} else {
			JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);

			SplitContext splitContext = SplitContext.create().indexFields(partitionIndex.getIndexFields()).total(count);
			SplitProgress splitProgress = SplitProgress.create().partitionFilter(TapPartitionFilter.create()).currentFieldPos(0).count(count);

//			if(currentPartitionCollector == null) {
//				currentPartitionCollector = rootPartitionCollector;
//			}

			handleReadPartitionScheduler = Executors.newSingleThreadScheduledExecutor();
//			if(partitionFuture != null)
//				partitionFuture.cancel(true);
//			partitionFuture = handleReadPartitionScheduler.scheduleWithFixedDelay(this::handleReadPartitions,  1000L, 1000L, TimeUnit.MILLISECONDS);

//			parallelWorker = asyncMaster.createAsyncParallelWorker(id, countNumOfThread);
//			parallelWorker.job(JobContext.create(splitProgress).context(splitContext), asyncQueueWorker -> asyncQueueWorker.job(this::handleJob).finished());
//			parallelWorker.start();

			handleJob(JobContext.create(splitProgress).context(splitContext));
			handleReadPartitions();

//			if(partitionFuture != null)
//				partitionFuture.cancel(true);
			//noinspection ResultOfMethodCallIgnored
//			handleReadPartitions();
			if(splitCompleteListener != null)
				splitCompleteListener.completed(id);
			context.getLog().info(id + ": Split job done because worker has finished");
		}
	}

	private synchronized void handleReadPartitions() {
		if(rootPartitionCollector.getState() == PartitionCollector.STATE_DONE) {
			handleReadPartitionPrivate(rootPartitionCollector, null, null);
//			parallelWorker.finished(() -> {
//				if(partitionFuture != null)
//					partitionFuture.cancel(true);
//				//noinspection ResultOfMethodCallIgnored
//				handleReadPartitions();
//				if(splitCompleteListener != null)
//					splitCompleteListener.completed(id);
//				context.getLog().info(id + ": Split job done because worker has finished");
//			});
		}
	}

	private void handleReadPartitionPrivate(PartitionCollector currentPartitionCollector, Map<TapPartitionFilter, Long> gatherFilters, Long total) {
		if(gatherFilters == null)
			gatherFilters = new LinkedHashMap<>();
		if(total == null)
			total = 0L;
		while (currentPartitionCollector != null && currentPartitionCollector.getState() == PartitionCollector.STATE_DONE) {
			Map<TapPartitionFilter, Long> partitionFilterLongMap = currentPartitionCollector.getPartitionCountMap();
			for(Map.Entry<TapPartitionFilter, Long> entry : partitionFilterLongMap.entrySet()) {
				if(countIsSlow) { //countIsSlow = true
					//unknown count for this partition
					if(entry.getValue() < 0) {
						if(!gatherFilters.isEmpty()) {
							ReadPartition readPartition1 = getReadPartition(gatherFilters, total);
							consumer.accept(readPartition1);

							gatherFilters.clear();
							total = 0L;
						}

						ReadPartition readPartition1 = ReadPartition.create().id(UUID.randomUUID().toString().replace("-", "")).partitionFilter(entry.getKey());
//						context.getLog().info(id + ": ReadPartition is ready to start reading for no count process, {}", readPartition1);
						consumer.accept(readPartition1);
					} else {
						gatherFilters.put(entry.getKey(), entry.getValue());
						total += entry.getValue();
					}
				} else {//countIsSlow = false
					gatherFilters.put(entry.getKey(), entry.getValue());
					total += entry.getValue();

					if(total >= maxRecordInPartition) {
						ReadPartition readPartition1 = getReadPartition(gatherFilters, total);
						consumer.accept(readPartition1);

						gatherFilters.clear();
						total = 0L;
					}
				}
			}
			PartitionCollector nextSplit = currentPartitionCollector.getNextSplit();
			PartitionCollector nextIndex = currentPartitionCollector.getNextIndex();
			if(nextSplit != null) {
				handleReadPartitionPrivate(nextSplit, gatherFilters, total);
			} else if(nextIndex != null) {
				if(!gatherFilters.isEmpty()) {
					ReadPartition readPartition1 = getReadPartition(gatherFilters, total);
					consumer.accept(readPartition1);

					gatherFilters.clear();
					total = 0L;
				}

				handleReadPartitionPrivate(nextIndex, null, null);
			}
			currentPartitionCollector = currentPartitionCollector.getSibling();
//			this.currentPartitionCollector = currentPartitionCollector;
		}
		if(!gatherFilters.isEmpty()) {
			ReadPartition readPartition1 = getReadPartition(gatherFilters, total);
			consumer.accept(readPartition1);

			gatherFilters.clear();
			total = 0L;
		}
	}

	private ReadPartition getReadPartition(Map<TapPartitionFilter, Long> gatherFilters, long total) {
		List<TapPartitionFilter> filters = new ArrayList<>(gatherFilters.keySet());
		TapPartitionFilter left = filters.get(0);
		TapPartitionFilter right = filters.get(filters.size() - 1);
		TapPartitionFilter finalFilter = TapPartitionFilter.create().resetMatch(left.getMatch()).leftBoundary(left.getLeftBoundary()).rightBoundary(right.getRightBoundary());

		ReadPartition readPartition1 = ReadPartition.create().id(UUID.randomUUID().toString().replace("-", "")).partitionFilter(finalFilter);
		context.getLog().info(id + ": ReadPartition is ready to start reading, {}, partition count {}", readPartition1, total);
		return readPartition1;
	}

	private JobContext handleJob(JobContext jobContext) {
		SplitContext splitContext = jobContext.getContext(SplitContext.class);
		SplitProgress splitProgress = jobContext.getResult(SplitProgress.class);
		TapPartitionFilter partitionFilter = splitProgress.getPartitionFilter();
		PartitionCollector partitionCollector = splitProgress.getPartitionCollector();
		if(partitionCollector == null) {
			partitionCollector = rootPartitionCollector;
		}

		context.getLog().info(id + " " + partitionFilter + ": start splitting");
		long count = splitProgress.getCount();
		List<TapIndexField> indexFields = splitContext.getIndexFields();
		long splitPieces = 100; //split 200 pieces by default. When count is < 0, default pieces will be used.
		if(count >= 0) {
			splitPieces = count / maxRecordInPartition + ((count % maxRecordInPartition) == 0 ? 0 : 1);
			context.getLog().info(id + " " + partitionFilter + ": Split into {} pieces", splitPieces);
		} else {
			context.getLog().info(id + " " + partitionFilter + ": Split into {} pieces by default as count is too slow", splitPieces);
		}

		TapIndexField indexField = indexFields.get(splitProgress.getCurrentFieldPos());
		FieldMinMaxValue fieldMinMaxValue = queryFieldMinMaxValue.minMaxValue(context, table, partitionFilter, indexField.getName());
		if(fieldMinMaxValue == null || fieldMinMaxValue.getMin() == null || fieldMinMaxValue.getMax() == null) {
			partitionCollector.addPartition(partitionFilter, 0L);
			partitionCollector.state(PartitionCollector.STATE_DONE);
			context.getLog().info("Partition {} can not find min/max value which means no record in table.", partitionFilter);
			return null;
		}
		String type = fieldMinMaxValue.getType();
		TypeSplitter<?> typeSplitter = typeSplitterMap.get(type);
		if(typeSplitter == null)
			throw new CoreException(ConnectorErrors.MISSING_TYPE_SPLITTER, "Missing type splitter for type {}", type);
		partitionCollector.state(PartitionCollector.STATE_MIN_MAX);

		Map<TapPartitionFilter, Long> minMaxPartitionMap = new HashMap<>();

		List<TapPartitionFilter> partitionFilters = typeSplitter.split(partitionFilter, fieldMinMaxValue, (int) splitPieces);
		if(countIsSlow) {
			long finalTimes = splitPieces;
			boolean noMoreSplit = false;
			long minTimes = splitPieces / 2;
			long time = System.currentTimeMillis();
			while(!noMoreSplit && partitionFilters.size() < minTimes) {
				context.getLog().info(id + " [countIsSlow] current partition size {} is less than minimum split pieces {}, will continue split to satisfy split pieces {} or can NOT split any more. ", partitionFilters.size(), minTimes, minTimes);
				List<TapPartitionFilter> newPartitionFilters = new ArrayList<>();
				AtomicBoolean canSplit = new AtomicBoolean(false);
				jobContext.foreach(partitionFilters, eachPartitionFilter -> {
					boolean noBoundary = eachPartitionFilter.getRightBoundary() == null && eachPartitionFilter.getLeftBoundary() == null && eachPartitionFilter.getMatch() != null;
					if(noBoundary) {
						newPartitionFilters.add(eachPartitionFilter);
						return true;
					}

					if(eachPartitionFilter.equals(partitionFilter)) {
						newPartitionFilters.add(eachPartitionFilter);
						return true;
					}

					FieldMinMaxValue fieldMinMaxValueForPartition = queryFieldMinMaxValue.minMaxValue(context, table, eachPartitionFilter, indexField.getName());
					if(fieldMinMaxValueForPartition == null || fieldMinMaxValueForPartition.getMin() == null || fieldMinMaxValueForPartition.getMax() == null) {
						newPartitionFilters.add(eachPartitionFilter);
						minMaxPartitionMap.put(eachPartitionFilter, 0L);
						return true;
					} else {
						minMaxPartitionMap.put(eachPartitionFilter, -1L);
					}

					String typeForPartition = fieldMinMaxValueForPartition.getType();
					TypeSplitter<?> typeSplitterForPartition = typeSplitterMap.get(typeForPartition);
					if(typeSplitterForPartition == null)
						throw new CoreException(ConnectorErrors.MISSING_TYPE_SPLITTER, "Missing type splitter for type {}", typeForPartition);

					List<TapPartitionFilter> partitionFiltersForPartition = typeSplitterForPartition.split(eachPartitionFilter, fieldMinMaxValueForPartition, (int) finalTimes);
					canSplit.set(true);
					newPartitionFilters.addAll(partitionFiltersForPartition);
					return true;
				});
				noMoreSplit = !canSplit.get();
				partitionFilters = newPartitionFilters;
			}
//			context.getLog().info(id + " [countIsSlow] Finished split partitionFilter {}, current partition size {}, minimum split pieces {}, noMoreSplit {} takes {} milliseconds", partitionFilter, partitionFilters.size(), minTimes, noMoreSplit, (System.currentTimeMillis() - time));
		}
		partitionCollector.state(PartitionCollector.STATE_SPLIT);

		context.getLog().info(id + " start collect all partitions with possible count");
		long time = System.currentTimeMillis();
		AtomicReference<PartitionCollector> partitionCollectorRef = new AtomicReference<>(partitionCollector);
		jobContext.foreach(partitionFilters, eachPartitionFilter -> {
			long partitionCount = -1;
			if(!countIsSlow) {
				partitionCount = countByPartitionFilter.countByPartitionFilter(context, table, eachPartitionFilter);
//				context.getLog().info(id + " " + partitionFilter + ": Partition count {} for {}", partitionCount, eachPartitionFilter);
			} else if(!minMaxPartitionMap.containsKey(eachPartitionFilter)){
				FieldMinMaxValue fieldMinMaxValueForPartition = queryFieldMinMaxValue.minMaxValue(context, table, eachPartitionFilter, indexField.getName());
				if(fieldMinMaxValueForPartition == null || fieldMinMaxValueForPartition.getMin() == null || fieldMinMaxValueForPartition.getMax() == null) {
					minMaxPartitionMap.put(eachPartitionFilter, 0L);
				} else {
					minMaxPartitionMap.put(eachPartitionFilter, -1L);
				}
			}

			boolean noBoundary = eachPartitionFilter.getRightBoundary() == null && eachPartitionFilter.getLeftBoundary() == null && eachPartitionFilter.getMatch() != null;
			partitionCollectorRef.get().setState(PartitionCollector.STATE_COUNT);
			Long eachCount = minMaxPartitionMap.get(eachPartitionFilter);
			boolean isEmptyPartition = eachCount != null && eachCount == 0;
			if(!isEmptyPartition && (noBoundary || (partitionCount >= 0 && partitionCount > maxRecordWithRatioInPartition))) {
				PartitionCollector newPartitionCollector = new PartitionCollector().state(PartitionCollector.STATE_COUNT);
				partitionCollectorRef.get().sibling(newPartitionCollector);
				partitionCollectorRef.get().state(PartitionCollector.STATE_DONE);

				if(noBoundary) { // which means min == max case.
					int pos = splitProgress.getCurrentFieldPos() + 1;
					if(indexFields.size() > pos) { // split into next index position.
						newPartitionCollector.nextIndex(new PartitionCollector()).state(PartitionCollector.STATE_DONE);
						SplitProgress newSplitProgress = SplitProgress.create().partitionCollector(newPartitionCollector.getNextIndex()).partitionFilter(eachPartitionFilter).currentFieldPos(splitProgress.getCurrentFieldPos() + 1).count(partitionCount);
//						parallelWorker.job(JobContext.create(newSplitProgress).context(splitContext), asyncQueueWorker -> asyncQueueWorker.job(this::handleJob).finished());
						handleJob(JobContext.create(newSplitProgress).context(splitContext));
					} else { // no more next index, make it a partition.
						newPartitionCollector.addPartition(eachPartitionFilter, partitionCount);
						newPartitionCollector.state(PartitionCollector.STATE_DONE);
					}
				} else { // still can be split in current index position. min != max case.
					newPartitionCollector.nextSplit(new PartitionCollector()).state(PartitionCollector.STATE_DONE);
					SplitProgress newSplitProgress = SplitProgress.create().partitionCollector(newPartitionCollector.getNextSplit()).partitionFilter(eachPartitionFilter).currentFieldPos(splitProgress.getCurrentFieldPos()).count(partitionCount);
//					parallelWorker.job(JobContext.create(newSplitProgress).context(splitContext), asyncQueueWorker -> asyncQueueWorker.job(this::handleJob).finished());
					handleJob(JobContext.create(newSplitProgress).context(splitContext));
				}

				PartitionCollector siblingCollector = new PartitionCollector().state(PartitionCollector.STATE_COUNT);
				newPartitionCollector.sibling(siblingCollector);
				partitionCollectorRef.set(siblingCollector);
			} else { // no need for split
				if(isEmptyPartition) {
					partitionCollectorRef.get().addPartition(eachPartitionFilter, 0L);
				} else {
					partitionCollectorRef.get().addPartition(eachPartitionFilter, partitionCount);
				}
			}
			return true;
		});
		partitionCollectorRef.get().state(PartitionCollector.STATE_DONE);
		context.getLog().info(id + " collected all partitions with possible count, takes {}", (System.currentTimeMillis() - time));
		return null;
	}

	public TapTable getTable() {
		return table;
	}

	public long getMaxRecordInPartition() {
		return maxRecordInPartition;
	}

	public List<ReadPartition> getExistingPartitions() {
		return existingPartitions;
	}

	public Consumer<ReadPartition> getConsumer() {
		return consumer;
	}

	public CountByPartitionFilterFunction getCountByPartitionFilter() {
		return countByPartitionFilter;
	}

	public QueryFieldMinMaxValueFunction getQueryFieldMinMaxValue() {
		return queryFieldMinMaxValue;
	}

	public TapConnectorContext getContext() {
		return context;
	}


	public String getId() {
		return id;
	}

	public int getMaxRecordRatio() {
		return maxRecordRatio;
	}

	public boolean isCountIsSlow() {
		return countIsSlow;
	}

	public int getCountNumOfThread() {
		return countNumOfThread;
	}

	public long getMaxRecordWithRatioInPartition() {
		return maxRecordWithRatioInPartition;
	}

	public int getSplitPiecesForCountIsSlow() {
		return splitPiecesForCountIsSlow;
	}
}

