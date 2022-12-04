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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * @author aplomb
 */
public class DatabaseReadPartitionSplitter {
	private static final String TAG = DatabaseReadPartitionSplitter.class.getSimpleName();
	private final Map<String, TypeSplitter> typeSplitterMap = new ConcurrentHashMap<>();
	private final String id;
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

		TapLogger.info(TAG, "Start splitting for table {}, maxRecordInPartition {}", table.getId(), maxRecordInPartition);
		TapPartitionFilter partitionFilter = TapPartitionFilter.create();
		long time = System.currentTimeMillis();
		long count = countByPartitionFilter.countByPartitionFilter(context, table, partitionFilter);
		TapLogger.info(TAG, "Initial count {}, takes {}", count, (System.currentTimeMillis() - time));

		TapIndexEx partitionIndex = table.partitionIndex();
		long max = maxRecordInPartition * 4;
		final String prefix = "P_";
		int index = 0;
		if(partitionIndex == null || max > count) {
			consumer.accept(ReadPartition.create().id(prefix + index).partitionFilter(TapPartitionFilter.create()));
		} else {
			AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);

			SplitContext splitContext = SplitContext.create().indexFields(partitionIndex.getIndexFields()).currentFieldPos(0).total(count);
			AtomicInteger jobIdGenerator = new AtomicInteger(0);
			AsyncParallelWorker parallelWorker = asyncMaster.createAsyncParallelWorker(id, 3);
			parallelWorker.start("root", JobContext.create(null).context(splitContext), asyncQueueWorker -> {
				asyncQueueWorker.job(String.valueOf(jobIdGenerator.incrementAndGet()), this::handleJob);
			});
		}
	}

	private JobContext handleJob(JobContext jobContext) {
		SplitContext splitContext = jobContext.getContext(SplitContext.class);

		long count = splitContext.getTotal();
		List<TapIndexField> indexFields = splitContext.getIndexFields();
		long times = count / maxRecordInPartition + ((count % maxRecordInPartition) == 0 ? 0 : 1);

		TapIndexField indexField = indexFields.get(splitContext.getCurrentFieldPos());
		FieldMinMaxValue fieldMinMaxValue = queryFieldMinMaxValue.minMaxValue(context, table, TapPartitionFilter.create(), indexField.getName());
		String type = fieldMinMaxValue.getType();
		TypeSplitter typeSplitter = typeSplitterMap.get(type);
		if(typeSplitter == null)
			throw new CoreException(ConnectorErrors.MISSING_TYPE_SPLITTER, "Missing type splitter for type {}", type);
		List<TapPartitionFilter> partitionFilters = typeSplitter.split(fieldMinMaxValue, (int) times);
		Map<TapPartitionFilter, Long> partitionCountMap = new LinkedHashMap<>();
		jobContext.foreach(partitionFilters, partitionFilter -> {
			partitionCountMap.put(partitionFilter, countByPartitionFilter.countByPartitionFilter(context, table, partitionFilter));
			return true;
		});
//		TapPartitionFilter partitionFilter = null;
//		Long partitionCount = 0L;
//		for(Map.Entry<TapPartitionFilter, Long> entry : partitionCountMap.entrySet()) {
//			if(partitionCount + entry.getValue() < maxRecordInPartition) {
//				partitionCount += entry.getValue();
//				if(partitionFilter == null) {
//					partitionFilter = entry.getKey();
//				} else {
//					List<QueryOperator> operators = partitionFilter.getOperators();
//					List<QueryOperator> newOperators = entry.getKey().getOperators();
//					if(operators != null && !operators.isEmpty() && newOperators != null && !newOperators.isEmpty()) {
//						if(operators.size() == 1 && operators.get(0).getOperator() == QueryOperator.LT && newOperators.size() == 2 && newOperators.get(0).getOperator() == QueryOperator.GTE) {
//							operators.remove(0);
//							operators.add(newOperators.get(1));
//						} else if(operators.size() == 2 && operators.get(0).getOperator() == QueryOperator.GTE) {
//							operators.remove(0);
//						}
//					}
//					if(newOperators != null )
//
//				}
//			} else if(entry.getValue() < 4 * maxRecordInPartition) {
//
//			} else {
//
//			}
//
//		}

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
}
