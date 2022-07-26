package io.tapdata.script;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.tapdata.constant.JSONUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DkMongoData {

	private static String path;
	private static String key;
	private static String mongoUri;
	private static String json;
	private static List<Map<String, Object>> list;

	private final static String NOT_FOUND_FIELD_LOG = "Not found field [%s] in map, index: %s.";
	private final static int DEFAULT_COLUMN_COUNT = 10;
	private final static int INSERT_BATCH_SIZE = 5000;

	public static void main(String[] args) {

		if (args.length >= 1) {
			path = StringUtils.isNoneBlank(args[0]) ? args[0].trim() : "";
		}

		if (args.length >= 2) {
			key = StringUtils.isNotBlank(args[1]) ? args[1].trim() : "";
		}

		if (args.length >= 3) {
			mongoUri = StringUtils.isNoneBlank(args[2]) ? args[2].trim() : "";
		}

		if (check()) {

			if (!readFile2JsonStr()) {
				return;
			}

			if (!getListFromJsonStr()) {
				return;
			}

			if (!insertDataByList()) {
				return;
			}

		} else {
			System.out.println(String.format("Missing parameters\n  - 1. %s\n  - 2. %s\n  - 3. %s", "json file path", "base key in json", "mongodb uri"));
		}
	}

	private static boolean readFile2JsonStr() {
		try (
				InputStream inputStream = new FileInputStream(path)
		) {

			StringBuffer jsonBuffer = new StringBuffer();
			byte[] buffer = new byte[2048];
			while (inputStream.read(buffer) != -1) {
				jsonBuffer.append(new String(buffer));
			}

			json = jsonBuffer.toString().trim();

		} catch (FileNotFoundException e) {
			System.out.println(String.format("%s - File not found.", path));
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			System.out.println(String.format("%s - Read file error: %s.", path, e.getMessage()));
			e.printStackTrace();
			return false;
		}

		return true;
	}

	private static boolean getListFromJsonStr() {
		if (StringUtils.isNotBlank(json)) {
			System.out.println(String.format("Finished read json from file: %s, length: %s.", path, json.length()));
			try {
				System.out.println("Starting convert json string to map...");
				Map<String, Object> map = JSONUtil.json2Map(json);
				if (MapUtils.isNotEmpty(map)) {
					System.out.println(String.format("Finished convert json string to map."));
					try {
						list = (List<Map<String, Object>>) map.get(key);

						System.out.println(String.format("Finished get list from map, list size: %s.", list.size()));
					} catch (Exception e) {
						System.out.println(String.format("Get list from map error: %s.", e.getMessage()));
						e.printStackTrace();
						return false;
					}
				} else {
					System.out.println(String.format("Map is empty."));
					return false;
				}
			} catch (IOException e) {
				System.out.println(String.format("Json string convert to map error: %s", e.getMessage()));
				e.printStackTrace();
				return false;
			}
		} else {
			System.out.println("File is empty.");
		}

		return true;
	}

	private static boolean insertDataByList() {
		if (CollectionUtils.isNotEmpty(list)) {

			MongoClientURI uri = new MongoClientURI(mongoUri);
			String database = uri.getDatabase();

			try (
					MongoClient mongoClient = new MongoClient(uri)
			) {
				int index = 0;
				for (Map<String, Object> collectionMap : list) {
					index++;
					long count;
					int avgObjSize;
					int columnCount = DEFAULT_COLUMN_COUNT;
					int columnLength;
					String collectionName;

					if (collectionMap.containsKey("count")) {
						count = Long.parseLong(collectionMap.get("count") + "");
					} else {
						System.out.println(String.format(NOT_FOUND_FIELD_LOG, "count", index));
						continue;
					}

					if (collectionMap.containsKey("avgObjSize")) {
						avgObjSize = (int) collectionMap.get("avgObjSize");
					} else {
						System.out.println(String.format(NOT_FOUND_FIELD_LOG, "avgObjSize", index));
						continue;
					}

					if (collectionMap.containsKey("schema")) {
						try {
							Map<String, Object> schema = (Map<String, Object>) collectionMap.get("schema");
							columnCount = schema.size() - 1;
						} catch (Exception e) {
							// do nothing
						}
					}
					columnLength = (int) (avgObjSize / columnCount * 0.55);
					collectionName = "collection_" + index + "_" + count;

					MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
					MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
					List<WriteModel<Document>> writeModels = new ArrayList<>();

					System.out.println(String.format("Insert data into collection: %s, rows: %s, avgObjSize: %s, column count: %s, column length: %s(B)"
							, collectionName
							, count
							, avgObjSize
							, columnCount
							, columnLength));

					long insertCount = 0;
					for (int i = 1; i <= count; i++) {
						Document document = new Document();
						for (int j = 1; j <= columnCount; j++) {
							document.put("k" + j, RandomStringUtils.randomAlphabetic(columnLength));
						}

						writeModels.add(new InsertOneModel(document));

						if (i % INSERT_BATCH_SIZE == 0) {
							BulkWriteResult bulkWriteResult = collection.bulkWrite(writeModels);
							insertCount += bulkWriteResult.getInsertedCount();
							writeModels.clear();
						}
					}

					if (CollectionUtils.isNotEmpty(writeModels)) {
						BulkWriteResult bulkWriteResult = collection.bulkWrite(writeModels);
						insertCount += bulkWriteResult.getInsertedCount();
					}

					System.out.println(String.format("Finished insert data into collection: %s, insert count: %s."
							, collectionName
							, insertCount));
				}
			}

		} else {
			System.out.println("List is empty.");
			return false;
		}
		return true;
	}

	private static boolean check() {
		return StringUtils.isNotBlank(path)
				&& StringUtils.isNotBlank(key)
				&& StringUtils.isNotBlank(mongoUri);
	}

}
