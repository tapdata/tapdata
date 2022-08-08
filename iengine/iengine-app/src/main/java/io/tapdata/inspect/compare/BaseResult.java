package io.tapdata.inspect.compare;

import com.tapdata.constant.MapUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.RelateDatabaseField;
import io.tapdata.ConverterProvider;
import io.tapdata.schema.SchemaList;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.io.Closeable;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/8/18 1:08 下午
 * @description
 */
public abstract class BaseResult<T> implements Closeable, Iterator<T> {

	protected long total;
	protected long pointer;
	protected List<String> sortColumns;
	protected Connections connections;
	protected String tableName;
	private RelateDataBaseTable tableSchema;
	protected Map<String, RelateDatabaseField> fields;
	protected ConverterProvider converterProvider;

	public BaseResult(List<String> sortColumns, Connections connections, String tableName) {
		this.sortColumns = sortColumns;
		this.connections = connections;
		this.tableName = tableName;
	}

	public BaseResult(List<String> sortColumns, Connections connections, String tableName, ConverterProvider converterProvider) {
		if (CollectionUtils.isEmpty(sortColumns)) {
			throw new IllegalArgumentException("Sort columns cannot be empty");
		}

		if (connections == null) {
			throw new IllegalArgumentException("Connections cannot be empty");
		}

		if (tableName == null) {
			throw new IllegalArgumentException("Table name cannot be empty");
		}
		this.sortColumns = sortColumns;
		this.connections = connections;
		this.tableName = tableName;

		if (connections.getSchema() != null && connections.getSchema().get("tables") != null) {
			List<RelateDataBaseTable> tables = connections.getSchema().get("tables");
			this.tableSchema = ((SchemaList<String, RelateDataBaseTable>) tables).get(tableName);
			if (this.tableSchema != null) {
				this.fields = this.tableSchema.getFields().stream()
						.collect(Collectors.toMap(RelateDatabaseField::getField_name, field -> field, (field1, field2) -> field1));
			} else {
				this.fields = null;
			}
		} else {
			this.tableSchema = null;
			this.fields = null;
		}
		this.converterProvider = converterProvider;
	}

	/**
	 * Get sort value, if sort column size>1, then join values split with comma
	 * e.g. record={a: 1, b: 2, c: 3}, sortColumns=["a", "b"], will return "1,2"
	 *
	 * @param record
	 * @return
	 */
	String getSortValue(Map<String, Object> record) {
		if (MapUtils.isEmpty(record)) {
			return null;
		}

		List<String> values = new LinkedList<>();
		sortColumns.stream().forEach(c -> {
			Object valueByKey = MapUtil.getValueByKey(record, c);
			if (valueByKey == null) {
				valueByKey = "null";
			}

			if (valueByKey instanceof Double || valueByKey instanceof Float) {
				valueByKey = new BigDecimal(valueByKey + "");
			}

			if (valueByKey instanceof BigDecimal) {
				valueByKey = ((BigDecimal) valueByKey).stripTrailingZeros().toPlainString();
			} else if (valueByKey instanceof byte[]) {
				valueByKey = Hex.encodeHexString((byte[]) valueByKey);
			}
			values.add(valueByKey.toString());
		});
		return String.join(",", values);
	}

	RelateDatabaseField getFieldSchema(String fieldName) {
		if (tableSchema != null && fieldName != null) {
			return fields.get(fieldName);
		}
		return null;
	}

	abstract long getTotal();

	abstract long getPointer();

	public List<String> getSortColumns() {
		return sortColumns;
	}
}
