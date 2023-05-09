package io.tapdata.schema;

import com.tapdata.entity.RelateDataBaseTable;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author samuel
 * @Description
 * @create 2021-11-09 16:49
 **/
public class SchemaMap extends HashMap<String, List<RelateDataBaseTable>> {

	private SchemaContext schemaContext;
	private SchemaList<String, RelateDataBaseTable> schemaList;

	SchemaMap(int initialCapacity, float loadFactor, SchemaContext schemaContext) {
		super(initialCapacity, loadFactor);
		this.schemaContext = schemaContext;
		schemaList = new SchemaList<>(schemaContext);
	}

	SchemaMap(int initialCapacity, SchemaContext schemaContext) {
		super(initialCapacity);
		this.schemaContext = schemaContext;
		schemaList = new SchemaList<>(schemaContext);
	}

	SchemaMap(SchemaContext schemaContext) {
		this.schemaContext = schemaContext;
		schemaList = new SchemaList<>(schemaContext);
	}

	private SchemaMap(SchemaContext schemaContext, SchemaList<String, RelateDataBaseTable> schemaList) {
		this.schemaContext = schemaContext;
		this.schemaList = schemaList;
	}

	SchemaMap(Map<? extends String, ? extends List<RelateDataBaseTable>> m, SchemaContext schemaContext) {
		super(m);
		this.schemaContext = schemaContext;
		schemaList = new SchemaList<>(schemaContext);
	}

	public void addTableNames(List<String> tableNames) {
		schemaList.addTableNames(tableNames);
	}

	@Override
	public List<RelateDataBaseTable> get(Object key) {
		return schemaList;
	}

	public SchemaContext getSchemaContext() {
		return schemaContext;
	}

	@Override
	public int size() {
		return schemaList.size();
	}

	@Override
	public boolean isEmpty() {
		return schemaList.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return true;
	}

	@Override
	public List<RelateDataBaseTable> put(String key, List<RelateDataBaseTable> value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void putAll(Map<? extends String, ? extends List<RelateDataBaseTable>> m) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<RelateDataBaseTable> remove(Object key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		schemaList.clear();
	}

	@Override
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<String> keySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<List<RelateDataBaseTable>> values() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<Entry<String, List<RelateDataBaseTable>>> entrySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<RelateDataBaseTable> getOrDefault(Object key, List<RelateDataBaseTable> defaultValue) {
		if (schemaList == null || schemaList.isEmpty()) {
			return defaultValue;
		} else {
			return schemaList;
		}
	}

	@Override
	public List<RelateDataBaseTable> putIfAbsent(String key, List<RelateDataBaseTable> value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object key, Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean replace(String key, List<RelateDataBaseTable> oldValue, List<RelateDataBaseTable> newValue) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<RelateDataBaseTable> replace(String key, List<RelateDataBaseTable> value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<RelateDataBaseTable> computeIfAbsent(String key, Function<? super String, ? extends List<RelateDataBaseTable>> mappingFunction) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<RelateDataBaseTable> computeIfPresent(String key, BiFunction<? super String, ? super List<RelateDataBaseTable>, ? extends List<RelateDataBaseTable>> remappingFunction) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<RelateDataBaseTable> compute(String key, BiFunction<? super String, ? super List<RelateDataBaseTable>, ? extends List<RelateDataBaseTable>> remappingFunction) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<RelateDataBaseTable> merge(String key, List<RelateDataBaseTable> value, BiFunction<? super List<RelateDataBaseTable>, ? super List<RelateDataBaseTable>, ? extends List<RelateDataBaseTable>> remappingFunction) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void forEach(BiConsumer<? super String, ? super List<RelateDataBaseTable>> action) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void replaceAll(BiFunction<? super String, ? super List<RelateDataBaseTable>, ? extends List<RelateDataBaseTable>> function) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object clone() {
		new SchemaMap(schemaContext, (SchemaList<String, RelateDataBaseTable>) schemaList.clone());
		return super.clone();
	}
}
