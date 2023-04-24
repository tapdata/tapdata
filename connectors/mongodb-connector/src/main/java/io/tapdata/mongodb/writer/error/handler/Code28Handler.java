package io.tapdata.mongodb.writer.error.handler;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.WriteError;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import io.tapdata.mongodb.writer.BulkWriteModel;
import io.tapdata.mongodb.writer.error.BulkWriteErrorHandler;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author samuel
 * @Description Cannot create field 'subField' in element {field: null}
 * If current document like {_id: xxx, field: null}, and you want to update subField of field use follow mql, you will get this error
 * db.collection.update({_id: xxx}, {$set: {'field.subField': 'xxx'}})
 * This handler can only process top level field
 * @create 2023-04-23 19:08
 **/
public class Code28Handler implements BulkWriteErrorHandler {

	public static final Pattern PATTERN = Pattern.compile("Cannot create field '(.*?)' in element \\{(.*?): null}");

	@Override
	public WriteModel<Document> handle(
			BulkWriteModel bulkWriteModel,
			WriteModel<Document> writeModel,
			BulkWriteOptions bulkWriteOptions,
			MongoBulkWriteException mongoBulkWriteException,
			BulkWriteError writeError,
			MongoCollection<Document> collection
	) {
		Document update = null;

		if (writeModel instanceof UpdateOneModel) {
			update = (Document) ((UpdateOneModel<Document>) writeModel).getUpdate();
		}
		if (writeModel instanceof UpdateManyModel) {
			update = (Document) ((UpdateManyModel<Document>) writeModel).getUpdate();
		}

		if (update == null || !update.containsKey("$set") || !(update.get("$set") instanceof Map)) {
			return null;
		}
		Map<String, Object> set = (Map) update.get("$set");
		String errorField = getErrorField(writeError);

		if (MapUtils.isEmpty(set) || StringUtils.isBlank(errorField)) {
			return null;
		}

		if (set.keySet().stream().anyMatch(key -> (
				// Is there only one layer
				StringUtils.startsWith(key, errorField) && StringUtils.countMatches(key, ".") > 1)
				// Whether there is a single parent node field name
				|| key.equals(errorField))) {
			return null;
		}

		Map<String, Object> subMap = new HashMap<>();
		Iterator<String> iterator = set.keySet().iterator();
		while (iterator.hasNext()) {
			String key = iterator.next();
			Object value = set.get(key);

			if (!StringUtils.startsWith(key, errorField + ".")) {
				continue;
			}

			String subKey = StringUtils.removeStart(key, errorField + ".");
			subMap.put(subKey, value);
			iterator.remove();
		}

		if (MapUtils.isNotEmpty(subMap)) {
			set.put(errorField, subMap);
		} else {
			return null;
		}
		return writeModel;
	}

	private String getErrorField(WriteError writeError) {
		if (null == writeError || StringUtils.isBlank(writeError.getMessage())) {
			return null;
		}
		Matcher matcher = PATTERN.matcher(writeError.getMessage());
		if (matcher.find() && matcher.groupCount() >= 1) {
			return matcher.group(2);
		} else {
			return null;
		}
	}
}
