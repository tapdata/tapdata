package io.tapdata.inspect.compare;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.tapdata.entity.Connections;
import com.tapdata.entity.RelateDatabaseField;
import io.tapdata.ConverterProvider;
import io.tapdata.exception.ConvertException;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/8/18 1:09 下午
 * @description
 */
public class MongoResult extends BaseResult<Map<String, Object>> {
	MongoClient mongoClient;
	MongoCursor<Document> mongoCursor;

	public MongoResult(List<String> sortColumns, Connections connections, String tableName, ConverterProvider converterProvider) {
		super(sortColumns, connections, tableName, converterProvider);
	}

	@Override
	public void close() {
		if (mongoCursor != null) {
			mongoCursor.close();
		}
		if (mongoClient != null) {
			mongoClient.close();
		}
	}

	@Override
	public boolean hasNext() {
		return mongoCursor.hasNext();
	}

	@Override
	public Map<String, Object> next() {
		pointer++;
		Document next = mongoCursor.next();
		next.keySet().stream().forEach(k -> {
			Object value = next.get(k);
			if (value != null) {
				try {
					RelateDatabaseField fieldSchema = getFieldSchema(k);
					if (fieldSchema != null && converterProvider != null) {
						converterProvider.javaTypeConverter(fieldSchema);
						value = converterProvider.sourceValueConverter(fieldSchema, value);
						next.put(k, value);
					}
					if (value instanceof Date) {
						next.put(k, ((Date) value).toInstant().toString());
					} else if (value instanceof ObjectId) {
						next.put(k, ((ObjectId) value).toHexString());
					}
				} catch (ConvertException e) {
					e.printStackTrace();
				}
			}

		});
		return next;
	}

	@Override
	public long getTotal() {
		return total;
	}

	@Override
	public long getPointer() {
		return pointer;
	}
}
