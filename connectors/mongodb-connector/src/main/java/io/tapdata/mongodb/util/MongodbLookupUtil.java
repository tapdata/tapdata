package io.tapdata.mongodb.util;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoCollection;
import io.tapdata.constant.AppType;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.mongodb.MongodbUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2020-08-26 21:35
 **/
public class MongodbLookupUtil {

		private static final String DELETE_CACHE_KEY_PREFIX = "Delete_Cache_";

		public static void lookUpAndSaveDeleteMessage(List<TapRecordEvent> tapRecordEvents,
																									KVMap<Object> globalStateMap,
																									ConnectionString connectionString,
																									Collection<String> pks,
																									MongoCollection<Document> mongoCollection) throws Exception {
				String validate = lookupAndSaveValidate(tapRecordEvents, globalStateMap, connectionString, mongoCollection);
				if (EmptyKit.isNotBlank(validate)) {
						throw new IllegalArgumentException(validate);
				}

				// look up oneone delete documents
				List<Document> lookupDocuments = lookupDocuments(tapRecordEvents, connectionString, pks, mongoCollection);

				if (CollectionUtils.isEmpty(lookupDocuments)) {
						return;
				}

				for (Document lookupDocument : lookupDocuments) {
						globalStateMap.put(DELETE_CACHE_KEY_PREFIX + "_" + lookupDocument.getString("mongodbUri") + "_" + lookupDocument.getString("collectionName") + "_" + lookupDocument.get("_id"), lookupDocument.toJson());
				}
		}

		private static List<Document> lookupDocuments(List<TapRecordEvent> tapRecordEvents,
																									ConnectionString connectionString,
																									Collection<String> pks,
																									MongoCollection<Document> mongoCollection) throws Exception {
				List<Document> lookupDocuments = new ArrayList<>();

				String mongodbUri = MongodbUtil.getSimpleMongodbUri(connectionString);
				mongodbUri = handleMongodbUriSpecialChar(mongodbUri);

			if (EmptyKit.isBlank(mongodbUri)) {
						throw new Exception(String.format("Cannot get mongodb uri from target connection, connection uri: %s", connectionString));
				}

				for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
						if (tapRecordEvent instanceof TapDeleteRecordEvent) {
								final Map<String, Object> deleteRow = ((TapDeleteRecordEvent) tapRecordEvent).getBefore();
								if (MapUtils.isEmpty(deleteRow)) {
										continue;
								}

								if (CollectionUtils.isEmpty(pks)) {
										continue;
								}

								Document lookupFilter = new Document();
								pks.forEach(pk -> {
										Object value = MapUtil.getValueByKey(deleteRow, pk);
										lookupFilter.append(pk, value);
								});
								Document lookupDocument = mongoCollection.find(lookupFilter).limit(1).iterator().tryNext();
								if (lookupDocument != null) {
										Document mongodbDeleteCache = new Document()
														.append("mongodbUri", mongodbUri)
														.append("_id", lookupDocument.get("_id"))
														.append("collectionName", mongoCollection.getNamespace().getCollectionName())
														.append("timestamp", System.currentTimeMillis())
														.append("data", lookupDocument);

										lookupDocuments.add(mongodbDeleteCache);
								}
						}

				}
				return lookupDocuments;
		}

	private static String handleMongodbUriSpecialChar(String mongodbUri) {
		mongodbUri = mongodbUri.replaceAll(":", "_")
				.replaceAll("/", "_");
		return mongodbUri;
	}

	private static String lookupAndSaveValidate(List<TapRecordEvent> tapRecordEvents,
																								KVMap<Object> globalKVMap,
																								ConnectionString connectionString,
																								MongoCollection<Document> mongoCollection) {
				if (CollectionUtils.isEmpty(tapRecordEvents)) {
						return "Missing input args messages";
				}

				if (globalKVMap == null) {
						return "Missing input args client global KV map";
				}

				if (connectionString == null) {
						return "Missing input args target mongo connection string";
				}

				if (mongoCollection == null) {
						return "Missing input args target mongoCollection";
				}

				return "";
		}

		public static Map findDeleteCacheByOid(ConnectionString connectionString, String collectionName, Object id, KVMap<Object> globalStateMap) {
                if(AppType.init().isCloud()){
					return null;
				}
				String validate = findValidate(connectionString, collectionName, id, globalStateMap);
				if (EmptyKit.isNotBlank(validate)) {
						throw new IllegalArgumentException(validate);
				}

				String mongodbUri = MongodbUtil.getSimpleMongodbUri(connectionString);
				mongodbUri = handleMongodbUriSpecialChar(mongodbUri);

				final String json = (String) globalStateMap.get(DELETE_CACHE_KEY_PREFIX + "_" + mongodbUri + "_" + collectionName + "_" + id);
				if (EmptyKit.isNotBlank(json)) {
						return Document.parse(json);
				}
				return null;

		}

		private static String findValidate(ConnectionString connectionString, String collectionName, Object oid, KVMap<Object> globalStateMap) {
				if (connectionString == null) {
						return "Missing input args connectionString";
				}

				if (EmptyKit.isBlank(collectionName)) {
						return "Missing input args collectionName";
				}

				if (oid == null) {
						return "Do not delete the _id field when mongo replicates data to mongo. Otherwise, the deletion event will not be replicated.";
				}

				if (globalStateMap == null) {
						return "Missing input args global state map";
				}

				return "";
		}
}
