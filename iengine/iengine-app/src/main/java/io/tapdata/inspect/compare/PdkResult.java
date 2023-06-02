package io.tapdata.inspect.compare;

import com.tapdata.entity.Connections;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.Projection;
import io.tapdata.pdk.apis.entity.SortOn;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import org.apache.commons.collections.CollectionUtils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author samuel
 * @Description
 * @create 2022-06-06 17:39
 **/
public class PdkResult extends BaseResult<Map<String, Object>> {
	private static final int BATCH_SIZE = 1000;
	private static final String TAG = PdkResult.class.getSimpleName();
	private final ConnectorNode connectorNode;
	private final LinkedBlockingQueue<Map<String, Object>> queue = new LinkedBlockingQueue<>(100);
	private final List<SortOn> sortOnList = new LinkedList<>();
	private final QueryByAdvanceFilterFunction queryByAdvanceFilterFunction;
	private final TapTable tapTable;
	private Throwable throwable;
	private final AtomicBoolean hasNext;
	private final AtomicBoolean running;
	private final TapCodecsFilterManager codecsFilterManager;
	private final TapCodecsFilterManager defaultCodecsFilterManager;
	private final Projection projection;
	private int diffKeyIndex;
	private final List<String> dataKeys;
	private final List<List<Object>> diffKeyValues;
	private final AtomicReference<Thread> queryThreadAR = new AtomicReference<>();

	public PdkResult(List<String> sortColumns, Connections connections, String tableName, Set<String> columns, ConnectorNode connectorNode, boolean fullMatch, List<String> dataKeys, List<List<Object>> diffKeyValues) {
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
		if (null == tapTable) {
			throw new RuntimeException("Table '" + connections.getName() + "'.'" + tableName + "' not exists.");
		}
		this.hasNext = new AtomicBoolean(true);
		this.running = new AtomicBoolean(true);
		this.codecsFilterManager = connectorNode.getCodecsFilterManager();
		this.defaultCodecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());
		if (!fullMatch) {
			projection = new Projection();
			sortColumns.forEach(projection::include);
		} else if (null != columns && !columns.isEmpty()) {
			projection = new Projection();
			sortColumns.forEach(projection::include);
			columns.forEach(s -> {
				if (!sortColumns.contains(s)) projection.include(s);
			});
		} else {
			projection = null;
		}
		this.dataKeys = dataKeys;
		this.diffKeyValues = diffKeyValues;
		initTotal();
	}

	private void initTotal() {
		if (null == diffKeyValues) {
			ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
			BatchCountFunction batchCountFunction = connectorFunctions.getBatchCountFunction();
			if (null == batchCountFunction) {
				total = 0L;
				return;
			}
			PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_BATCH_COUNT,
					() -> total = batchCountFunction.count(connectorNode.getConnectorContext(), tapTable), TAG);
		} else {
			total = diffKeyValues.size();
		}
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
		return null != queryThreadAR.get() || queue.size() > 0;
	}

	@Override
	public Map<String, Object> next() {
		while (isRunning() && hasNext()) {
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
		return hasNext.get() && null == queryThreadAR.get();
	}

	private void queryNextBatch() {
		synchronized (queryThreadAR) {
			if (null == queryThreadAR.get()) {
				queryThreadAR.set(new Thread(() -> {
					Thread.currentThread().setName(String.format("INSPECT-QUERY-%s.%s", connections.getId(), tableName));
					try {
						TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create();

						// query one difference data, because pdk api not support 'or' conditions.
						if (null != diffKeyValues && diffKeyIndex < diffKeyValues.size()) {
							List<Object> objects = diffKeyValues.get(diffKeyIndex);
							if (objects.size() != dataKeys.size()) {
								throw new RuntimeException("The data key size not equals data value size: " + tapTable.getId());
							}
							for (int i = 0; i < dataKeys.size(); i++) {
								tapAdvanceFilter.match(DataMap.create().kv(dataKeys.get(i), objects.get(i)));
							}
							diffKeyIndex++;
						}

//						tapAdvanceFilter.setLimit(BATCH_SIZE); // can not add limit because queryByAdvanceFilterFunction not support 'or' conditions
						tapAdvanceFilter.setSortOnList(sortOnList);
						tapAdvanceFilter.setProjection(projection);
						PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
							() -> queryByAdvanceFilterFunction.query(connectorNode.getConnectorContext(), tapAdvanceFilter, tapTable, filterResults -> {
								Throwable error = filterResults.getError();
								if (null != error) throwable = error;

								List<Map<String, Object>> results = filterResults.getResults();
								if (CollectionUtils.isEmpty(results)) return;
								for (Map<String, Object> result : results) {
									if (!isRunning()) break;
									while (isRunning()) {
										try {
											if (queue.offer(result, 100L, TimeUnit.MILLISECONDS)) {
												break;
											}
										} catch (InterruptedException e) {
											return;
										}
									}
								}
							}), TAG);
						if (null == diffKeyValues || diffKeyIndex + 1 >= diffKeyValues.size()) {
							hasNext.set(false);
						}
					} catch (Exception e) {
						throwable = e;
					} finally {
						synchronized (queryThreadAR) {
							queryThreadAR.set(null);
						}
					}
				}));
				queryThreadAR.get().start();
			}
		}
	}

	private boolean isRunning() {
		return running.get() && !Thread.currentThread().isInterrupted();
	}
}
