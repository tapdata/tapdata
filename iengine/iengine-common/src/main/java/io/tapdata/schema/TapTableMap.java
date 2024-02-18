package io.tapdata.schema;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.error.TapTableMapExCode_29;
import io.tapdata.exception.TapCodeException;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.RetryUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description
 * @create 2022-05-10 11:16
 **/
public class TapTableMap<K extends String, V extends TapTable> extends HashMap<K, V> {
	protected final String nodeId;
	protected final Long time;
	protected final Map<K, String> tableNameAndQualifiedNameMap;
	private final Lock lock = new ReentrantLock();
	private TaskConfig taskConfig;
	private String nodeName;
	private final Logger logger = LogManager.getLogger(TapTableMap.class);
	public static final String PRELOAD_SCHEMA_WAIT_TIME = System.getenv().getOrDefault("PRELOAD_SCHEMA_WAIT_TIME","10");
	private TapLogger.LogListener logListener;
	protected TapTableMap(String nodeId, Long time, Map<K, String> tableNameAndQualifiedNameMap) {
		if (StringUtils.isBlank(nodeId)) {
			throw new RuntimeException("Missing node id");
		}
		this.nodeId = nodeId;
		this.time = time;
		this.tableNameAndQualifiedNameMap = new ConcurrentHashMap<>(tableNameAndQualifiedNameMap);
	}

	protected void initLogListener() {
		logListener = new TapLogger.LogListener() {
			@Override
			public void debug(String log) {
				logger.debug(log);
			}
			@Override
			public void info(String log) {
				logger.info(log);
			}
			@Override
			public void warn(String log) {
				logger.warn(log);
			}
			@Override
			public void error(String log) {
				logger.error(log);
			}
			@Override
			public void fatal(String log) {
				logger.fatal(log);
			}
			@Override
			public void memory(String memoryLog) {
			}
		};
	}

	public static TapTableMap<String, TapTable> create(String nodeId) {
		return create(null, nodeId);
	}

	public static TapTableMap<String, TapTable> create(String prefix, String nodeId) {
		return create(prefix, nodeId, new HashMap<>(), null);
	}

	public static TapTableMap<String, TapTable> create(String nodeId, Map<String, String> tableNameAndQualifiedNameMap) {
		return create(nodeId, tableNameAndQualifiedNameMap, null);
	}

	public static TapTableMap<String, TapTable> create(String nodeId, Map<String, String> tableNameAndQualifiedNameMap, Long time) {
		return create(null, nodeId, tableNameAndQualifiedNameMap, time);
	}

	public static TapTableMap<String, TapTable> create(String prefix, String nodeId, Map<String, String> tableNameAndQualifiedNameMap, Long time) {
		TapTableMap<String, TapTable> tapTableMap;
		if (tableNameAndQualifiedNameMap.size() > 99) {
			tapTableMap = new TapTableMapEhcache<>(prefix, nodeId, time, tableNameAndQualifiedNameMap);
		} else {
			tapTableMap = new TapTableMap<>(nodeId, time, tableNameAndQualifiedNameMap);
		}
		tapTableMap.initLogListener();
		return tapTableMap;
	}

	public static TapTableMap<String, TapTable> create(String nodeId, TapTable tapTable) {
		return create(nodeId, Collections.singletonList(tapTable), null);
	}

	public static TapTableMap<String, TapTable> create(String nodeId, List<TapTable> tapTableList, Long time) {
		return create(null, nodeId, tapTableList, time);
	}

	public static TapTableMap<String, TapTable> create(String prefix, String nodeId, List<TapTable> tapTableList, Long time) {
		HashMap<String, String> tableNameAndQualifiedNameMap = new HashMap<>();
		for (TapTable tapTable : tapTableList) {
			tableNameAndQualifiedNameMap.put(tapTable.getName(), tapTable.getId());
		}
		TapTableMap<String, TapTable> tapTableMap = create(prefix, nodeId, tableNameAndQualifiedNameMap, time);
		for (TapTable tapTable : tapTableList) {
			tapTableMap.put(tapTable.getId(), tapTable);
		}
		return tapTableMap;
	}

	public String getQualifiedName(K tableName) {
		return tableNameAndQualifiedNameMap.get(tableName);
	}

	@Override
	public int size() {
		return tableNameAndQualifiedNameMap.size();
	}

	@Override
	public boolean isEmpty() {
		return tableNameAndQualifiedNameMap.isEmpty();
	}

	public TapTableMap logListener(TapLogger.LogListener logListener){
		this.logListener = logListener;
		return this;
	}
	public void buildTaskRetryConfig(TaskConfig taskConfig){
		this.taskConfig = taskConfig;
	}
	public void buildNodeName(String nodeName){
		this.nodeName = nodeName;
	}
	@Override
	public final V get(Object key) {
		if (null == taskConfig || null == taskConfig.getTaskRetryConfig()){
			return getTapTable((K) key);
		}
		AtomicReference<V> res = new AtomicReference<>();
		PDKMethodInvoker invoker = PDKMethodInvoker.create()
				.logTag(TapTableMap.class.getSimpleName())
				.logListener(logListener)
				.retryPeriodSeconds(taskConfig.getTaskRetryConfig().getRetryIntervalSecond())
				.maxRetryTimeMinute(taskConfig.getTaskRetryConfig().getMaxRetryTime(TimeUnit.MINUTES))
				.runnable(()->res.set(getTapTable((K) key)));
		PDKInvocationMonitor.invokerRetrySetter(invoker);
		RetryUtils.autoRetry(PDKMethod.IENGINE_FIND_SCHEMA, invoker);
		return res.get();
	}

	private CompletableFuture<Void> future = null;
	private ExecutorService executorService = null;
	public void preLoadSchema() {
		logListener.info(String.format("Node %s[%s] start preload schema,table counts: %d", this.nodeName, this.nodeId, tableNameAndQualifiedNameMap.size()));
		long start = System.currentTimeMillis();
		List<String> tableNames = new ArrayList<>(tableNameAndQualifiedNameMap.keySet());
		AtomicInteger index = new AtomicInteger(0);
		AtomicLong allCostTs = new AtomicLong(0L);
		int cursor = preLoadSchema(tableNames, index.get(), costTs -> allCostTs.addAndGet(costTs) > TimeUnit.SECONDS.toMillis(Long.parseLong(PRELOAD_SCHEMA_WAIT_TIME)), start);
		index.set(cursor);
		if (index.get() == size()) return;
		logListener.info(String.format("Node %s[%s] preload schema will fork continue, remind counts: %d", this.nodeName, this.nodeId, tableNameAndQualifiedNameMap.size()-index.get()));
		executorService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
		future = CompletableFuture.runAsync(() -> {
			try {
				Thread.currentThread().setName("Node ["+this.nodeId+"]"  + "-preload-schema-runner");
				preLoadSchema(tableNames, index.get(), null, start);
			}catch (Exception e){
				logListener.warn(String.format("Node %s[%s] preload schema failed: %s\n%s", this.nodeName, this.nodeId, e.getMessage(),Log4jUtil.getStackString(e)));
			}finally {
				executorService.shutdown();
			}
		}, executorService);
	}
	protected int preLoadSchema(List<String> tableNames, int index, Function<Long, Boolean> costInterceptor, long start) {
		for (int i = index; i < tableNames.size(); i++) {
			if (Thread.currentThread().isInterrupted()) {
				break;
			}
			long startTs = System.currentTimeMillis();
			String tableName = tableNames.get(i);
			getTapTable((K) tableName);
			index++;
			long endTs = System.currentTimeMillis();
			long costTs = endTs - startTs;
			if (null != costInterceptor) {
				Boolean isIntercept = costInterceptor.apply(costTs);
				if (Boolean.TRUE.equals(isIntercept)) {
					break;
				}
			}
		}
		if (index == tableNames.size()){
			long end = System.currentTimeMillis();
			logListener.info(String.format("Node %s[%s] preload schema finished, cost %d ms", this.nodeName, this.nodeId, end-start));
		}
		return index;
	}

	public void doClose() {
		//停止预加载线程
		//未执行完
		if (null != future && !future.isDone() && null != executorService){
			future.cancel(true);
			executorService.shutdown();
		}
	}
	@Override
	public final boolean containsKey(Object key) {
		return tableNameAndQualifiedNameMap.containsKey(key);
	}

	@Override
	public final V put(K key, V value) {
		if (!tableNameAndQualifiedNameMap.containsKey(key)) {
			throw new IllegalArgumentException("Table " + key + " does not exists, cannot put in table map");
		}
		putTapTable(key, value);
		return value;
	}

	public final void putNew(K key, V value, String qualifiedName) {
		if (StringUtils.isBlank(qualifiedName)) {
			throw new IllegalArgumentException("Qualified name is blank, table id: " + key + ", schema: " + value);
		}
		this.tableNameAndQualifiedNameMap.put(key, qualifiedName);
		putTapTable(key, value);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		throw new UnsupportedOperationException();
	}

	@Override
	public final V remove(Object key) {
		this.tableNameAndQualifiedNameMap.remove(key);
		return removeTapTable((K) key);
	}

	@Override
	public final void clear() {
		this.tableNameAndQualifiedNameMap.clear();
		clearTapTable();
	}

	@Override
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public final Set<K> keySet() {
		return tableNameAndQualifiedNameMap.keySet();
	}

	@Override
	public Collection<V> values() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public V getOrDefault(Object key, V defaultValue) {
		if (containsKey(key)) {
			return get(key);
		} else {
			return defaultValue;
		}
	}

	@Override
	public V putIfAbsent(K key, V value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object key, Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean replace(K key, V oldValue, V newValue) {
		throw new UnsupportedOperationException();
	}

	@Override
	public V replace(K key, V value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
		throw new UnsupportedOperationException();
	}

	@Override
	public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
		throw new UnsupportedOperationException();
	}

	@Override
	public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
		throw new UnsupportedOperationException();
	}

	@Override
	public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
		throw new UnsupportedOperationException();
	}

	@Override
	public final void forEach(BiConsumer<? super K, ? super V> action) {
		for (K k : tableNameAndQualifiedNameMap.keySet()) {
			V v = get(k);
			action.accept(k, v);
		}
	}

	@Override
	public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
		throw new UnsupportedOperationException();
	}

	protected V getTapTable(K key) {
		V tapTable = super.get(key);
		if (null == tapTable) {
			try {
				tapTable = handleWithLock(() -> {
					V tmp = super.get(key);
					if (null == tmp) {
						tmp = findSchema(key);
						super.put(key, tmp);
					}
					return tmp;
				});
			} catch (Exception e) {
				throw new RuntimeException("Find schema failed, message: " + e.getMessage(), e);
			}
		}
		return tapTable;
	}

	protected void putTapTable(K key, V value) {
		super.put(key, value);
	}

	protected V removeTapTable(K key) {
		return super.remove(key);
	}

	protected void clearTapTable() {
		super.clear();
	}

	protected V findSchema(K k) {
		String qualifiedName = tableNameAndQualifiedNameMap.get(k);
		if (StringUtils.isBlank(qualifiedName)) {
			if (null != k && k.contains(".")) {
				String[] split = k.split("\\.");
				k = (K) split[split.length - 1];
			}
			if (null != k && ConnHeartbeatUtils.TABLE_NAME.contentEquals(k)) {
				qualifiedName = TapTableUtil.getHeartbeatQualifiedName(nodeId);
			}
			if (StringUtils.isBlank(qualifiedName)) {
				throw new RuntimeException("Table name \"" + k + "\" not exists, qualified name: " + qualifiedName
					+ " tableNameAndQualifiedNameMap: " + tableNameAndQualifiedNameMap);
			}
		}
		ClientMongoOperator clientMongoOperator = createClientMongoOperator();
		String url;
		Query query;
		TapTable tapTable;
		try {
			if (null != time && time.compareTo(0L) > 0) {
				url = ConnectorConstant.METADATA_HISTROY_COLLECTION;
				Map<String, Object> param = new HashMap<>();
				param.put("qualifiedName", qualifiedName);
				param.put("time", time);
				tapTable = clientMongoOperator.findOne(param, url, TapTable.class);
			} else {
				url = ConnectorConstant.METADATA_INSTANCE_COLLECTION + "/tapTables";
				query = Query.query(where("qualified_name").is(qualifiedName));
				tapTable = clientMongoOperator.findOne(query, url, TapTable.class);
			}
		}catch (Exception e){
			throw new TapCodeException(TapTableMapExCode_29.FIND_SCHEMA_FAILED, String.format("Table [%s] find schema failed", k), e);
		}
		if (null == tapTable) {
			throw new RuntimeException("Table name \"" + k + "\" not exists, qualified name: " + qualifiedName);
		}

		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		if (MapUtils.isNotEmpty(nameFieldMap)) {
			LinkedHashMap<String, TapField> sortedFieldMap = new LinkedHashMap<>();
			nameFieldMap.entrySet().stream().sorted((o1, o2) -> {
				Integer o1Pos = o1.getValue().getPos();
				Integer o2Pos = o2.getValue().getPos();
				if (o1Pos == null && o2Pos == null) {
					return 0;
				}

				if (o1Pos == null) {
					return -1;
				}

				if (o2Pos == null) {
					return 1;
				}
				return o1Pos.compareTo(o2Pos);
			}).forEach(entry -> sortedFieldMap.put(entry.getKey(), entry.getValue()));
			tapTable.setNameFieldMap(sortedFieldMap);
		}
		return (V) tapTable;
	}

	protected ClientMongoOperator createClientMongoOperator() {
		return BeanUtil.getBean(ClientMongoOperator.class);
	}

	protected <T> T handleWithLock(Supplier<T> supplier) throws Exception {
		try {
			while (!Thread.currentThread().isInterrupted()) {
				if (lock.tryLock(3, TimeUnit.SECONDS)) {
					break;
				}
			}
			return supplier.get();
		} finally {
			lock.unlock();
		}
	}

	public void reset() {
		resetTapTable();
		this.tableNameAndQualifiedNameMap.clear();
		doClose();
	}

	protected void resetTapTable() {
		super.clear();
	}

	public Iterator<io.tapdata.entity.utils.cache.Entry<TapTable>> iterator() {
		java.util.Iterator<K> iterator = tableNameAndQualifiedNameMap.keySet().iterator();
		return new Iterator<io.tapdata.entity.utils.cache.Entry<TapTable>>() {

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public io.tapdata.entity.utils.cache.Entry<TapTable> next() {
				String tableName = iterator.next();
				//noinspection unchecked
				TapTable tapTable = getTapTable((K) tableName);
				return new io.tapdata.entity.utils.cache.Entry<TapTable>() {
					@Override
					public String getKey() {
						return tableName;
					}

					@Override
					public TapTable getValue() {
						return tapTable;
					}
				};
			}
		};
	}
}
