package com.tapdata.mongo.error.handler;

import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.tapdata.mongo.error.BulkWriteErrorHandler;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-08-25 16:20
 **/
public class Code28Handler implements BulkWriteErrorHandler {

	private final static String ERROR_MESSAGE_PREFIX_REGEX = "Cannot create field '.*' in element \\{";
	private final static String ERROR_MESSAGE_SUFFIX_REGEX = ": null\\}";

	/**
	 * The target value is null. When a.b is used to update the embedded document, the error will be triggered
	 * Solution: Change the form of a.b to a: {b: value}
	 * Only supports one layer
	 *
	 * @param writeModel     {@link WriteModel}
	 * @param bulkWriteError {@link BulkWriteError}
	 */
	@Override
	public boolean handle(WriteModel<Document> writeModel, BulkWriteError bulkWriteError) {
		if (writeModel == null) {
			return false;
		}

		Document update = null;

		if (writeModel instanceof UpdateOneModel) {
			update = (Document) ((UpdateOneModel<Document>) writeModel).getUpdate();
		}
		if (writeModel instanceof UpdateManyModel) {
			update = (Document) ((UpdateManyModel<Document>) writeModel).getUpdate();
		}

		if (update == null || !update.containsKey("$set") || !(update.get("$set") instanceof Map)) {
			return false;
		}

		Map<String, Object> set = (Map) update.get("$set");
		String errorField = getErrorField(bulkWriteError);

		if (MapUtils.isEmpty(set) || StringUtils.isBlank(errorField)) {
			return false;
		}

		if (set.keySet().stream().anyMatch(key -> (
				// Is there only one layer
				StringUtils.startsWith(key, errorField) && StringUtils.countMatches(key, ".") > 1)
				// Whether there is a single parent node field name
				|| key.equals(errorField))) {
			return false;
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
		}

		return true;
	}

	private String getErrorField(BulkWriteError bulkWriteError) {
		if (bulkWriteError == null) {
			return null;
		}

		String message = bulkWriteError.getMessage();
		message = message.replaceAll(ERROR_MESSAGE_PREFIX_REGEX, "");
		message = message.replaceAll(ERROR_MESSAGE_SUFFIX_REGEX, "");

		return message;
	}
}
