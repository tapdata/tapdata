package io.tapdata.inspect.compare;

import com.tapdata.entity.Connections;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.Projection;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.SortOn;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import org.apache.commons.collections.CollectionUtils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author samuel
 * @Description
 * @create 2022-06-06 17:39
 **/
public class PdkResult extends BaseResult<Map<String, Object>> {
	private static final int BATCH_SIZE = 1000;
	private static final String TAG = PdkResult.class.getSimpleName();
	private ConnectorNode connectorNode;
	private LinkedBlockingQueue<Map<String, Object>> queue = new LinkedBlockingQueue<>();
	private DataMap offset;
	private List<SortOn> sortOnList = new LinkedList<>();
	private QueryByAdvanceFilterFunction queryByAdvanceFilterFunction;
	private TapTable tapTable;
	private Throwable throwable;
	private AtomicBoolean hasNext;
	private AtomicBoolean running;
	private TapCodecsFilterManager codecsFilterManager;
	private TapCodecsFilterManager defaultCodecsFilterManager;
	private Projection projection;

	public PdkResult(List<String> sortColumns, Connections connections, String tableName, ConnectorNode connectorNode, boolean fullMatch) {
		super(sortColumns, connections, tableName);
		this.connectorNode = connectorNode;
		for (String sortColumn : sortColumns) {
			sortOnList.add(SortOn.ascending(sortColumn));
		}
		queryByAdvanceFilterFunction = connectorNode.getConnectorFunctions().getQueryByAdvanceFilterFunction();
		if (null == queryByAdvanceFilterFunction) {
			throw new RuntimeException("Connector does not support query by filter function: " + connectorNode.getConnectorContext().getSpecification().getId());
		}
		this.tapTable = connectorNode.getConnectorContext().getTableMap().get(tableName);
		this.hasNext = new AtomicBoolean(true);
		this.running = new AtomicBoolean(true);
		this.codecsFilterManager = connectorNode.getCodecsFilterManager();
		this.defaultCodecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());
		if (!fullMatch) {
			projection = new Projection();
			sortColumns.forEach(s -> projection.include(s));
		} else {
			projection = null;
		}
		initTotal();
	}

	private void initTotal() {
		ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
		BatchCountFunction batchCountFunction = connectorFunctions.getBatchCountFunction();
		if (null == batchCountFunction) {
			total = 0L;
			return;
		}
		PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_BATCH_COUNT,
				() -> total = batchCountFunction.count(connectorNode.getConnectorContext(), tapTable), TAG);
	}

	@Override
	long getTotal() {
		return total;
	}

	@Override
	long getPointer() {
		return pointer;
	}

	@Override
	public void close() throws IOException {
		running.set(false);
	}

	@Override
	public boolean hasNext() {
		if (null != throwable) throw new RuntimeException(throwable);
		offerInQueue();
		return hasNext.get();
	}

	@Override
	public Map<String, Object> next() {
		if (!hasNext()) {
			return null;
		}
		while (isRunning()) {
			try {
				Map<String, Object> poll = queue.poll(100L, TimeUnit.MILLISECONDS);
				if (null != poll) {
					pointer++;
					codecsFilterManager.transformToTapValueMap(poll, tapTable.getNameFieldMap());
					defaultCodecsFilterManager.transformFromTapValueMap(poll);
					return poll;
				}
			} catch (InterruptedException e) {
				break;
			}
		}
		return null;
	}

	private void offerInQueue() {
		if (needQuery()) {
			queryNextBatch();
		}
	}

	private boolean needQuery() {
		return hasNext.get() && queue.size() == 0;
	}

	private void queryNextBatch() {
		TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create();
		if (null == offset) {
			offset = new DataMap();
		} else {
			List<QueryOperator> operators = new LinkedList<>();
			offset.forEach((k, v) -> operators.add(QueryOperator.gt(k, v)));
			tapAdvanceFilter.setOperators(operators);
		}
		tapAdvanceFilter.setLimit(BATCH_SIZE);
		tapAdvanceFilter.setSortOnList(sortOnList);
		tapAdvanceFilter.setProjection(projection);
		PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
				() -> queryByAdvanceFilterFunction.query(connectorNode.getConnectorContext(), tapAdvanceFilter, tapTable, filterResults -> {
					Throwable error = filterResults.getError();
					if (null != error) throwable = error;
					List<Map<String, Object>> results = filterResults.getResults();
					if (CollectionUtils.isEmpty(results)) {
						hasNext.set(false);
						return;
					}
					for (Map<String, Object> result : results) {
						if (!isRunning()) break;
						while (isRunning()) {
							try {
								if (queue.offer(result, 100L, TimeUnit.MILLISECONDS)) {
									sortColumns.forEach(sortColumn -> offset.put(sortColumn, result.get(sortColumn)));
									break;
								}
							} catch (InterruptedException e) {
								return;
							}
						}
					}
				}), TAG);
		if (queue.size() == 0) {
			hasNext.set(false);
		}
	}

	private boolean isRunning() {
		return running.get() && !Thread.currentThread().isInterrupted();
	}
}
