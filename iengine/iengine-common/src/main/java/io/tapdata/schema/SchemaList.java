package io.tapdata.schema;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.RelateDatabaseField;
import com.tapdata.entity.TableIndex;
import com.tapdata.entity.TableIndexColumn;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2021-11-09 17:23
 **/
public class SchemaList<T extends String, E extends RelateDataBaseTable> extends ArrayList<E> {

	private static Logger logger = LogManager.getLogger(SchemaList.class);

	private final static long LOCK_TIMEOUT = 30 * 60 * 1000L;

	private SchemaContext schemaContext;
	private Set<T> tableNames = new HashSet<>();
	private LRUMap<T, E> tableMap = new LRUMap<>(100);

	private Lock tableMapLock = new ReentrantLock();

	SchemaList(int initialCapacity, SchemaContext schemaContext) {
		super(initialCapacity);
		this.schemaContext = schemaContext;
	}

	SchemaList(SchemaContext schemaContext) {
		this.schemaContext = schemaContext;
	}

	SchemaList(Collection<? extends E> c, SchemaContext schemaContext) {
		super(c);
		this.schemaContext = schemaContext;
	}

	private SchemaList(SchemaContext schemaContext, Set<T> tableNames, LRUMap<T, E> tableMap) {
		this.schemaContext = schemaContext;
		this.tableNames = tableNames;
		this.tableMap = tableMap;
	}

	@Override
	public E get(int index) {
		if (index > (tableNames.size() - 1)) {
			throw new ArrayIndexOutOfBoundsException("Index " + index + " is invalid, max index: " + (tableNames.size() - 1));
		}
		T t = null;
		int i = 0;
		for (T name : tableNames) {
			if (i == index) {
				t = name;
				break;
			}
			i++;
		}
		if (StringUtils.isBlank(t)) {
			throw new RuntimeException("Cannot find table name with index: " + index);
		}
		return getSchema(t);
	}

	public void addTableNames(List<T> tableNames) {
		try {
			if (tableMapLock.tryLock(LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
				tableNames = tableNames.parallelStream().filter(StringUtils::isNotBlank).collect(Collectors.toList());
				this.tableNames.addAll(tableNames);
			} else {
				throw new RuntimeException("Add schema failed; Get lock time out");
			}
		} catch (InterruptedException ignore) {
			return;
		} finally {
			try {
				tableMapLock.unlock();
			} catch (Exception ignore) {
			}
		}
	}

	@Override
	public Iterator<E> iterator() {
		return new Itr();
	}

	@Override
	public void forEach(Consumer<? super E> action) {
		tableNames.forEach(t -> {
			E e = getSchema(t);
			action.accept(e);
		});
	}

	public E get(T t) {
		return getSchema(t);
	}

	public List<RelateDatabaseField> getFields(T t) {
		E e = get(t);
		if (null == e || null == e.getFields()) {
			return null;
		}
		return e.getFields();
	}

	public void clearFields(T t) {
		removeSchema(t);
	}

	public Map<String, RelateDatabaseField> getFieldMap(T t) {
		E e = get(t);
		if (null == e || null == e.getFields()) {
			return null;
		}
		// in some cases(file/kafka), the filed_name might be duplicate
		return e.getFields().stream().collect(Collectors.toMap(
				RelateDatabaseField::getField_name,
				Function.identity(),
				(existing, replacement) -> existing
		));
	}

	public class Itr implements Iterator<E> {
		private Iterator<T> tableNamesIter;

		public Itr() {
			this.tableNamesIter = tableNames.iterator();
		}

		@Override
		public boolean hasNext() {
			return tableNamesIter.hasNext();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void forEachRemaining(Consumer<? super E> action) {
			throw new UnsupportedOperationException();
		}

		@Override
		public E next() {
			T t = tableNamesIter.next();
			return getSchema(t);
		}
	}

	private E getSchema(T t) {
		if (t == null) {
			return null;
		}
		E e = tableMap.get(t);
		if (null == e) {
			try {
				if (tableMapLock.tryLock(LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
					e = tableMap.get(t);
					if (null == e) {
						e = findSchema(t);
						if (null == e) {
							return null;
						}
						tableMap.put(t, e);
						tableNames.add((T) e.getTable_name());
					}
				} else {
					throw new RuntimeException("Get schema failed; Cannot get lock: " + t);
				}
			} catch (InterruptedException ignore) {
				return null;
			} finally {
				try {
					tableMapLock.unlock();
				} catch (Exception ignore) {
				}
			}
		}
		return e;
	}

	public Set<T> getTableNames() {
		return tableNames;
	}

	private void removeSchema(T t) {
		tableMap.remove(t);
	}

	private E findSchema(String tableName) {
		if (logger.isDebugEnabled()) {
			logger.debug("Find metadata: " + schemaContext.getConnections().getId() + ", " + tableName);
		}
		ClientMongoOperator clientMongoOperator = schemaContext.getClientMongoOperator();
		if (clientMongoOperator == null) {
			throw new IllegalArgumentException("Client mongo operator cannot be null");
		}
		DatabaseTypeEnum databaseTypeEnum;
		String metaType;
		Connections connections = schemaContext.getConnections();
		try {
			databaseTypeEnum = DatabaseTypeEnum.fromString(connections.getDatabase_type());
		} catch (Exception e) {
			throw new RuntimeException("Get database type " + connections.getDatabase_type() + " failed; " + e.getMessage(), e);
		}
		if (null == databaseTypeEnum) {
			throw new RuntimeException("Get database type " + connections.getDatabase_type() + " failed; Not exists");
		}
		switch (databaseTypeEnum) {
			case MONGODB:
			case ALIYUN_MONGODB:
			case MQ:
			case DUMMY:
			case KAFKA:
				metaType = "collection";
				break;
			default:
				metaType = "table";
				break;
		}
		Map<String, Object> params = new HashMap<>();
		params.put("metaType", metaType);
		params.put("connectionId", connections.getId());
		params.put("tableName", tableName);
		RelateDataBaseTable relateDataBaseTable = clientMongoOperator.findOne(params, ConnectorConstant.METADATA_INSTANCE_COLLECTION + "/metadata", RelateDataBaseTable.class);
		return (E) relateDataBaseTable;
	}

	@Override
	public boolean isEmpty() {
		return tableNames.isEmpty();
	}

	@Override
	public int size() {
		return tableNames.size();
	}

	@Override
	public void trimToSize() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void ensureCapacity(int minCapacity) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean contains(Object o) {
		return tableNames.contains(o);
	}

	@Override
	public int indexOf(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int lastIndexOf(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object clone() {
		return new SchemaList<T, E>(schemaContext, new HashSet<>(tableNames), new LRUMap<>(tableMap));
	}

	@Override
	public Object[] toArray() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		throw new UnsupportedOperationException();
	}

	@Override
	public E set(int index, E element) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean add(E e) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void add(int index, E element) {
		throw new UnsupportedOperationException();
	}

	@Override
	public E remove(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		try {
			if (tableMapLock.tryLock(LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
				tableNames.clear();
				tableMap.clear();
			} else {
				throw new RuntimeException("Clear schema list failed; Get lock time out");
			}
		} catch (InterruptedException ignore) {
		} finally {
			try {
				tableMapLock.unlock();
			} catch (Exception ignore) {
			}
		}
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void removeRange(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ListIterator<E> listIterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Spliterator<E> spliterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeIf(Predicate<? super E> filter) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void replaceAll(UnaryOperator<E> operator) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void sort(Comparator<? super E> c) {
		throw new UnsupportedOperationException();
	}

	public List<RelateDatabaseField> getPrimaryKey(T t) {
		E schema = getSchema(t);
		List<RelateDatabaseField> fields = schema.getFields();
		return fields.stream().filter(f -> f.getPrimary_key_position() > 0).collect(Collectors.toList());
	}

	public List<TableIndex> getUniqueIndex(T t) {
		E schema = getSchema(t);
		List<TableIndex> indices = schema.getIndices();
		return indices.stream().filter(TableIndex::isUnique).collect(Collectors.toList());
	}

	public List<RelateDatabaseField> getPkOrUniqueField(T t) {
		List<RelateDatabaseField> primaryKey = getPrimaryKey(t);
		if (CollectionUtils.isNotEmpty(primaryKey)) {
			return primaryKey;
		}
		List<TableIndex> uniqueIndex = getUniqueIndex(t);
		List<RelateDatabaseField> uniqueFields = new ArrayList<>();
		Map<String, RelateDatabaseField> fieldMap = getFieldMap(t);
		uniqueIndex.forEach(u -> {
			List<TableIndexColumn> columns = u.getColumns();
			columns.forEach(c -> uniqueFields.add(fieldMap.get(c.getColumnName())));
		});
		return uniqueFields;
	}
}
