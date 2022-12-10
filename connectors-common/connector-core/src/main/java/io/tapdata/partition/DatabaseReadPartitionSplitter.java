package io.tapdata.partition;

import io.tapdata.async.master.*;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapIndexEx;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.error.ConnectorErrors;
import io.tapdata.partition.splitter.*;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.source.CountByPartitionFilterFunction;
import io.tapdata.pdk.apis.functions.connector.source.QueryFieldMinMaxValueFunction;
import io.tapdata.pdk.apis.partition.FieldMinMaxValue;
import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * @author aplomb
 */
public class DatabaseReadPartitionSplitter {
	private static final String TAG = "Split";
	private final Map<String, TypeSplitter> typeSplitterMap = new ConcurrentHashMap<>();
	private String id;
	public DatabaseReadPartitionSplitter id(String id) {
		this.id = id;
		return this;
	}
	private long maxRecordWithRatioInPartition;
	private AsyncParallelWorker parallelWorker;
	private AsyncQueueWorker readPartitionTimerTask;

	private final PartitionCollector rootPartitionCollector = new PartitionCollector();
	private PartitionCollector currentPartitionCollector;

	public DatabaseReadPartitionSplitter() {
		id = "DatabaseReadPartitionSplitter_" + UUID.randomUUID();
		typeSplitterMap.put(FieldMinMaxValue.TYPE_BOOLEAN, BooleanSplitter.INSTANCE);
		typeSplitterMap.put(FieldMinMaxValue.TYPE_DATE, DateTimeSplitter.INSTANCE);
		typeSplitterMap.put(FieldMinMaxValue.TYPE_NUMBER, NumberSplitter.INSTANCE);
		typeSplitterMap.put(FieldMinMaxValue.TYPE_STRING, StringSplitter.INSTANCE);
	}

	public DatabaseReadPartitionSplitter registerCustomSplitter(Class<?> clazz, TypeSplitter typeSplitter) {
		typeSplitterMap.put(clazz.getName(), typeSplitter);
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
		if(countByPartitionFilter == null)
			throw new CoreException(ConnectorErrors.MISSING_COUNT_BY_PARTITION_FILTER, "Missing countByPartitionFilter while startSplitting");
		if(queryFieldMinMaxValue == null)
			throw new CoreException(ConnectorErrors.MISSING_QUERY_FIELD_MIN_MAX_VALUE, "Missing queryFieldMinMaxValue while startSplitting");
		if(table == null)
			throw new CoreException(ConnectorErrors.MISSING_TABLE, "Missing table while startSplitting");
		if(consumer == null)
			throw new CoreException(ConnectorErrors.MISSING_CONSUMER, "Missing consumer while startSplitting");
		if(context == null)
			throw new CoreException(ConnectorErrors.MISSING_CONNECTOR_CONTEXT, "Missing connector context while startSplitting");

		TapLogger.info(TAG, id + ": Start splitting for table {}, maxRecordInPartition {}", table.getId(), maxRecordInPartition);
		TapPartitionFilter partitionFilter = TapPartitionFilter.create();
		long time = System.currentTimeMillis();
		long count = countIsSlow ? -1 : countByPartitionFilter.countByPartitionFilter(context, table, partitionFilter);
		TapLogger.info(TAG, id + ": Initial count {}, takes {} countIsSlow {}", count, (System.currentTimeMillis() - time), countIsSlow);

		TapIndexEx partitionIndex = table.partitionIndex();
		TapLogger.info(TAG, id + ": partitionIndex {}", partitionIndex);
		maxRecordWithRatioInPartition = maxRecordInPartition * maxRecordRatio;
		TapLogger.info(TAG, id + ": Record range for a partition is {} ~ {}", maxRecordInPartition, maxRecordWithRatioInPartition);
		final String prefix = "P_";
		int index = 0;
		if(partitionIndex == null || (count != -1 && maxRecordWithRatioInPartition > count)) {
			ReadPartition readPartition = ReadPartition.create().id(prefix + index).partitionFilter(TapPartitionFilter.create());
			TapLogger.info(TAG, id + ": Count {} less than max {}, will not split, but only on ReadPartition {}", count, maxRecordWithRatioInPartition, readPartition);
			consumer.accept(readPartition);
			if(splitCompleteListener != null)
				splitCompleteListener.completed(id);
			TapLogger.info(TAG, id + ": Split job done only one piece here, no need to split, maxRecordWithRatioInPartition {} partitionIndex {}", maxRecordWithRatioInPartition, partitionIndex);
		} else {
			AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);

			SplitContext splitContext = SplitContext.create().indexFields(partitionIndex.getIndexFields()).total(count);
			SplitProgress splitProgress = SplitProgress.create().partitionFilter(TapPartitionFilter.create()).currentFieldPos(0).count(count);

			if(currentPartitionCollector == null) {
				currentPartitionCollector = rootPartitionCollector;
			}

			readPartitionTimerTask = asyncMaster.createAsyncQueueWorker("readPartitionTimerTask", false);
			readPartitionTimerTask.job(this::handleReadPartitions).start(JobContext.create(null), 1000L, 1000L);
			parallelWorker = asyncMaster.createAsyncParallelWorker(id, countNumOfThread);
			parallelWorker.setParallelWorkerStateListener((id, fromState, toState) -> {
				if(toState == ParallelWorkerStateListener.STATE_LONG_IDLE) {
					if(readPartitionTimerTask != null)
						readPartitionTimerTask.stop();
					//noinspection ResultOfMethodCallIgnored
					handleReadPartitions(JobContext.create(null));
					if(splitCompleteListener != null)
						splitCompleteListener.completed(id);
					TapLogger.info(TAG, id + ": Split job done because worker has entered LONG IDLE state");
				}
			});
			parallelWorker.start(JobContext.create(splitProgress).context(splitContext), asyncQueueWorker -> asyncQueueWorker.job(this::handleJob));
		}
	}

	private synchronized JobContext handleReadPartitions(JobContext jobContext) {
		ReadPartition readPartition;
		List<TapPartitionFilter> gatherFilters = new ArrayList<>();
		long total = 0;
		while (currentPartitionCollector != null && currentPartitionCollector.getState() == PartitionCollector.STATE_DONE) {
			Map<TapPartitionFilter, Long> partitionFilterLongMap = currentPartitionCollector.getPartitionCountMap();
			if(partitionFilterLongMap != null) {
				for(Map.Entry<TapPartitionFilter, Long> entry : partitionFilterLongMap.entrySet()) {
					if(entry.getValue() < 0) { //countIsSlow = true
						//unknown count for this partition
						ReadPartition readPartition1 = ReadPartition.create().id(entry.getKey().toString()).partitionFilter(entry.getKey());
						TapLogger.info(TAG, id + ": ReadPartition is ready to start reading for no count process, {}", readPartition1);
						consumer.accept(readPartition1);
					} else {//countIsSlow = false
						gatherFilters.add(entry.getKey());
						total += entry.getValue();

						if(total >= maxRecordInPartition) {
							ReadPartition readPartition1 = getReadPartition(gatherFilters, total);
							consumer.accept(readPartition1);

							gatherFilters.clear();
							total = 0;
						}
					}
				}
				currentPartitionCollector = currentPartitionCollector.getNext();
			}
		}
		if(!gatherFilters.isEmpty()) {
			ReadPartition readPartition1 = getReadPartition(gatherFilters, total);
			consumer.accept(readPartition1);
		}
		return null;
	}

	private ReadPartition getReadPartition(List<TapPartitionFilter> gatherFilters, long total) {
		TapPartitionFilter left = gatherFilters.get(0);
		TapPartitionFilter right = gatherFilters.get(gatherFilters.size() - 1);
		TapPartitionFilter finalFilter = TapPartitionFilter.create().resetMatch(left.getMatch()).leftBoundary(left.getLeftBoundary()).rightBoundary(right.getRightBoundary());

		ReadPartition readPartition1 = ReadPartition.create().id(finalFilter.toString()).partitionFilter(finalFilter);
		TapLogger.info(TAG, id + ": ReadPartition is ready to start reading, {}, partition count {}", readPartition1, total);
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

		TapLogger.info(TAG, id + " " + partitionFilter + ": start splitting");
		long count = splitProgress.getCount();
		List<TapIndexField> indexFields = splitContext.getIndexFields();
		long times = 200; //split 200 pieces by default. When count is < 0, default pieces will be used.
		if(count >= 0) {
			times = count / maxRecordInPartition + ((count % maxRecordInPartition) == 0 ? 0 : 1);
			TapLogger.info(TAG, id + " " + partitionFilter + ": Split into {} pieces", times);
		} else {
			TapLogger.info(TAG, id + " " + partitionFilter + ": Split into {} pieces by default as count is too slow", times);
		}

		TapIndexField indexField = indexFields.get(splitProgress.getCurrentFieldPos());
		FieldMinMaxValue fieldMinMaxValue = queryFieldMinMaxValue.minMaxValue(context, table, partitionFilter, indexField.getName());
		String type = fieldMinMaxValue.getType();
		TypeSplitter typeSplitter = typeSplitterMap.get(type);
		if(typeSplitter == null)
			throw new CoreException(ConnectorErrors.MISSING_TYPE_SPLITTER, "Missing type splitter for type {}", type);
		partitionCollector.state(PartitionCollector.STATE_MIN_MAX);

		List<TapPartitionFilter> partitionFilters = typeSplitter.split(partitionFilter, fieldMinMaxValue, (int) times);
		partitionCollector.state(PartitionCollector.STATE_SPLIT);

		AtomicReference<PartitionCollector> partitionCollectorRef = new AtomicReference<>(partitionCollector);
		jobContext.foreach(partitionFilters, eachPartitionFilter -> {
			long partitionCount = -1;
			if(!countIsSlow) {
				partitionCount = countByPartitionFilter.countByPartitionFilter(context, table, eachPartitionFilter);
				TapLogger.info(TAG, id + " " + partitionFilter + ": Partition count {} for {}", partitionCount, eachPartitionFilter);
			}
			boolean noBoundary = eachPartitionFilter.getRightBoundary() == null && eachPartitionFilter.getLeftBoundary() == null && eachPartitionFilter.getMatch() != null;
			partitionCollectorRef.get().setState(PartitionCollector.STATE_COUNT);
			if(noBoundary || (partitionCount >= 0 && partitionCount > maxRecordWithRatioInPartition)) {
				PartitionCollector newPartitionCollector = new PartitionCollector();
				partitionCollectorRef.get().next(newPartitionCollector);
				partitionCollectorRef.get().state(PartitionCollector.STATE_DONE);

				if(noBoundary) {
					int pos = splitProgress.getCurrentFieldPos() + 1;
					if(indexFields.size() > pos) {
						SplitProgress newSplitProgress = SplitProgress.create().partitionCollector(newPartitionCollector).partitionFilter(eachPartitionFilter).currentFieldPos(splitProgress.getCurrentFieldPos() + 1).count(partitionCount);
						parallelWorker.start(JobContext.create(newSplitProgress).context(splitContext), asyncQueueWorker -> asyncQueueWorker.job(this::handleJob));
					} else {
						newPartitionCollector.addPartition(eachPartitionFilter, partitionCount);
						newPartitionCollector.state(PartitionCollector.STATE_DONE);
					}
				} else {
					SplitProgress newSplitProgress = SplitProgress.create().partitionCollector(newPartitionCollector).partitionFilter(eachPartitionFilter).currentFieldPos(splitProgress.getCurrentFieldPos()).count(partitionCount);
					parallelWorker.start(JobContext.create(newSplitProgress).context(splitContext), asyncQueueWorker -> asyncQueueWorker.job(this::handleJob));
				}

				PartitionCollector siblingCollector = new PartitionCollector().state(PartitionCollector.STATE_COUNT);
				newPartitionCollector.next(siblingCollector);
				partitionCollectorRef.set(siblingCollector);
			} else {
				partitionCollectorRef.get().addPartition(eachPartitionFilter, partitionCount);
			}
			return true;
		});
		partitionCollectorRef.get().state(PartitionCollector.STATE_DONE);
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

	public Map<String, TypeSplitter> getTypeSplitterMap() {
		return typeSplitterMap;
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


}
