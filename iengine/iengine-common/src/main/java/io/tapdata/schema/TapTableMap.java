package io.tapdata.schema;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;
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
	private Map<K, String> tableNameAndQualifiedNameMap = new ConcurrentHashMap<>();
	private LRUMap<K, V> tapTableLRUMap = new LRUMap<>(10);
	private Lock lock = new ReentrantLock();

	public TapTableMap(Map<K, String> tableNameAndQualifiedNameMap) {
		this.tableNameAndQualifiedNameMap.putAll(tableNameAndQualifiedNameMap);
	}

	public TapTableMap(String nodeId) {
		this.tableNameAndQualifiedNameMap = (Map<K, String>) TapTableUtil.getTableNameQualifiedNameMap(nodeId);
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
		return getTapTable((K) key);
	}

	@Override
	public boolean containsKey(Object key) {
		return tableNameAndQualifiedNameMap.containsKey(key);
	}

	@Override
	public V put(K key, V value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		throw new UnsupportedOperationException();
	}

	@Override
	public V remove(Object key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		this.tableNameAndQualifiedNameMap.clear();
		this.tapTableLRUMap.clear();
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
		return tapTableLRUMap.values();
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return tapTableLRUMap.entrySet();
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

	private V getTapTable(K key) {
		AtomicReference<V> tapTable = new AtomicReference<>(tapTableLRUMap.get(key));
		if (null == tapTable.get()) {
			try {
				handleWithLock(() -> {
					tapTable.set(tapTableLRUMap.get(key));
					if (null == tapTable.get()) {
						tapTable.set(findSchema(key));
						this.tapTableLRUMap.put(key, tapTable.get());
					}
				});
			} catch (Exception e) {
				throw new RuntimeException("Find schema failed, message: " + e.getMessage(), e);
			}
		}
		return tapTable.get();
	}

	private V findSchema(K k) {
		String qualifiedName = tableNameAndQualifiedNameMap.get(k);
		if (StringUtils.isBlank(qualifiedName)) {
			throw new RuntimeException("Table name \"" + k + "\" not exists, qualified name: " + qualifiedName);
		}
		ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
		String url = ConnectorConstant.METADATA_INSTANCE_COLLECTION + "/tapTables";
		Query query = Query.query(where("qualified_name").is(qualifiedName));
		TapTable tapTable = clientMongoOperator.findOne(query, url, TapTable.class);
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
}
