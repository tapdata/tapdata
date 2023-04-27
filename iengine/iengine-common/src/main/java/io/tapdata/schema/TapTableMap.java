package io.tapdata.schema;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
import io.tapdata.cache.EhcacheService;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.cache.EhcacheKVMap;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description
 * @create 2022-05-10 11:16
 **/
public class TapTableMap<K extends String, V extends TapTable> extends HashMap<K, V> {
	private static final String DIST_CACHE_PATH = "tap_table_ehcache";
	public static final int DEFAULT_OFF_HEAP_MB = 10;
	public static final int DEFAULT_DISK_MB = 1024;
	public static final int MAX_HEAP_ENTRIES = 100;
	public static final String TAP_TABLE_OFF_HEAP_MB_KEY = "TAP_TABLE_OFF_HEAP_MB";
	public static final String TAP_TABLE_DISK_MB_KEY = "TAP_TABLE_DISK_MB";
	public static final String TAP_TABLE_PREFIX = "TAP_TABLE_";
	private Map<K, String> tableNameAndQualifiedNameMap;
	private String mapKey;
	private Lock lock = new ReentrantLock();
	private String nodeId;
	private Long time;

	private TapTableMap() {

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
		TapTableMap<String, TapTable> tapTableMap = new TapTableMap<>();
		tapTableMap
				.nodeId(nodeId)
				.tableNameAndQualifiedNameMap(tableNameAndQualifiedNameMap)
				.time(time)
				.init(prefix);
		EhcacheService.getInstance().getEhcacheKVMap(tapTableMap.mapKey).clear();
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

	private TapTableMap<K, V> init(String prefix) {
		if (StringUtils.isBlank(nodeId)) {
			throw new RuntimeException("Missing node id");
		}
//		if (MapUtils.isEmpty(tableNameAndQualifiedNameMap)) {
//			throw new RuntimeException("Missing table name and qualified name map");
//		}
		this.mapKey = TAP_TABLE_PREFIX + nodeId;
		if (StringUtils.isNotEmpty(prefix)) {
			this.mapKey = prefix + "_" + this.mapKey;
		}
		createEhcacheMap();
		return this;
	}

	private void createEhcacheMap() {
		try {
			EhcacheKVMap<TapTable> tapTableMap = EhcacheKVMap.create(this.mapKey, TapTable.class)
					.cachePath(DIST_CACHE_PATH)
					.maxHeapEntries(MAX_HEAP_ENTRIES)
					//				.maxOffHeapMB(CommonUtils.getPropertyInt(TAP_TABLE_OFF_HEAP_MB_KEY, DEFAULT_OFF_HEAP_MB))
					.maxDiskMB(CommonUtils.getPropertyInt(TAP_TABLE_DISK_MB_KEY, DEFAULT_DISK_MB))
					.init();
			EhcacheService.getInstance().putEhcacheKVMap(mapKey, tapTableMap);
		} catch (Throwable e) {
			throw new RuntimeException(String.format("Failed to create Ehcache TapTableMap, node id: %s, map name: %s, error: %s", nodeId, mapKey, e.getMessage()));
		}
	}

	public String getQualifiedName(String tableName) {
		return tableNameAndQualifiedNameMap.get(tableName);
	}

	private TapTableMap<K, V> tableNameAndQualifiedNameMap(Map<K, String> tableNameAndQualifiedNameMap) {
		this.tableNameAndQualifiedNameMap = new ConcurrentHashMap<>(tableNameAndQualifiedNameMap);
		return this;
	}

	private TapTableMap<K, V> nodeId(String nodeId) {
		this.nodeId = nodeId;
		return this;
	}

	public TapTableMap<K, V> time(Long time) {
		this.time = time;
		return this;
	}

	@Override
	public int size() {
		return tableNameAndQualifiedNameMap.size();
	}

	@Override
	public boolean isEmpty() {
		return tableNameAndQualifiedNameMap.isEmpty();
	}

	@Override
	public V get(Object key) {
		return (V) getTapTable((K) key);
	}

	@Override
	public boolean containsKey(Object key) {
		return tableNameAndQualifiedNameMap.containsKey(key);
	}

	@Override
	public V put(K key, V value) {
		if (!tableNameAndQualifiedNameMap.containsKey(key)) {
			throw new IllegalArgumentException("Table " + key + " does not exists, cannot put in table map");
		}
		EhcacheService.getInstance().getEhcacheKVMap(mapKey).put(key, value);
		return value;
	}

	public void putNew(K key, V value, String qualifiedName) {
		if (StringUtils.isBlank(qualifiedName)) {
			throw new IllegalArgumentException("Qualified name is blank, table id: " + key + ", schema: " + value);
		}
		this.tableNameAndQualifiedNameMap.put(key, qualifiedName);
		EhcacheService.getInstance().getEhcacheKVMap(mapKey).put(key, value);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		throw new UnsupportedOperationException();
	}

	@Override
	public V remove(Object key) {
		this.tableNameAndQualifiedNameMap.remove(key);
		EhcacheService.getInstance().getEhcacheKVMap(mapKey).remove((String) key);
		return null;
	}

	@Override
	public void clear() {
		this.tableNameAndQualifiedNameMap.clear();
		EhcacheService.getInstance().getEhcacheKVMap(this.mapKey).clear();
	}

	@Override
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<K> keySet() {
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
	public void forEach(BiConsumer<? super K, ? super V> action) {
		for (K k : tableNameAndQualifiedNameMap.keySet()) {
			V v = get(k);
			action.accept(k, v);
		}
	}

	@Override
	public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
		throw new UnsupportedOperationException();
	}

	private TapTable getTapTable(K key) {
		AtomicReference<EhcacheKVMap<TapTable>> tapTableMap = new AtomicReference<>();
		tapTableMap.set(EhcacheService.getInstance().getEhcacheKVMap(this.mapKey));
		if (null == tapTableMap.get()) {
			try {
				handleWithLock(() -> {
					tapTableMap.set(EhcacheService.getInstance().getEhcacheKVMap(this.mapKey));
					if (null == tapTableMap.get()) {
						createEhcacheMap();
						tapTableMap.set(EhcacheService.getInstance().getEhcacheKVMap(this.mapKey));
					}
				});
			} catch (Throwable e) {
				throw new RuntimeException(String.format("Create TapTableMap failed, node id: %s, map name: %s, error: %s", nodeId, mapKey, e.getMessage()), e);
			}
		}
		if (null == tapTableMap.get()) {
			throw new IllegalArgumentException(String.format("Cannot create TapTableMap, node id: %s, map name: %s", nodeId, mapKey));
		}
		AtomicReference<TapTable> tapTable = new AtomicReference<>();
		if (null == tapTable.get()) {
			try {
				handleWithLock(() -> {
					tapTable.set(tapTableMap.get().get(key));
					if (null == tapTable.get()) {
						tapTable.set(findSchema(key));
						tapTableMap.get().put(key, tapTable.get());
					}
				});
			} catch (Throwable e) {
				throw new RuntimeException("Find schema failed, message: " + e.getMessage(), e);
			}
		}
		return tapTable.get();
	}

	private V findSchema(K k) {
		String qualifiedName = tableNameAndQualifiedNameMap.get(k);
		if (StringUtils.isBlank(qualifiedName)) {
			if (ConnHeartbeatUtils.TABLE_NAME.contentEquals(k)) {
				qualifiedName = TapTableUtil.getHeartbeatQualifiedName(nodeId);
			}
			if (StringUtils.isBlank(qualifiedName)) {
				throw new RuntimeException("Table name \"" + k + "\" not exists, qualified name: " + qualifiedName
						+ " tableNameAndQualifiedNameMap: " + tableNameAndQualifiedNameMap);
			}
		}
		ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
		String url;
		Query query;
		TapTable tapTable;
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

	private void handleWithLock(Handler handler) throws Exception {
		try {
			lock();
			handler.run();
		} finally {
			lock.unlock();
		}
	}

	private void lock() throws Exception {
		while (true) {
			if (Thread.currentThread().isInterrupted()) {
				break;
			}
			if (lock.tryLock(3, TimeUnit.SECONDS)) {
				break;
			}
		}
	}

	interface Handler {
		void run() throws Exception;
	}

	public void reset() {
		EhcacheService ehcacheService = EhcacheService.getInstance();
		if (StringUtils.isNotBlank(mapKey)) {
			EhcacheKVMap<Object> ehcacheKVMap = ehcacheService.getEhcacheKVMap(mapKey);
			Optional.ofNullable(ehcacheKVMap).ifPresent(EhcacheKVMap::reset);
			ehcacheService.removeEhcacheKVMap(mapKey);
		}
		this.tableNameAndQualifiedNameMap.clear();
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
