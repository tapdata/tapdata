package io.tapdata.schema;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.Iterator;
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

	protected TapTableMap(String nodeId, Long time, Map<K, String> tableNameAndQualifiedNameMap) {
		if (StringUtils.isBlank(nodeId)) {
			throw new RuntimeException("Missing node id");
		}
		this.nodeId = nodeId;
		this.time = time;
		this.tableNameAndQualifiedNameMap = new ConcurrentHashMap<>(tableNameAndQualifiedNameMap);
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
//			tapTableMap = new TapTableMapTapStorage<>(prefix, nodeId, time, tableNameAndQualifiedNameMap);
		} else {
			tapTableMap = new TapTableMap<>(nodeId, time, tableNameAndQualifiedNameMap);
		}
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

	@Override
	public final V get(Object key) {
		return getTapTable((K) key);
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
