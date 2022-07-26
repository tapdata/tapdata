package io.tapdata.task;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.DataRules;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonRegularExpression;
import org.bson.Document;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@TaskType(type = "MONGODB_CREATE_VALIDATOR")
public class MongodbCreateValidatorTask implements Task {


	private Map<String, Object> taskData;

	@Override
	public void initialize(TaskContext taskContext) {

		taskData = taskContext.getTaskData();
	}

	@Override
	public void execute(Consumer<TaskResult> callback) {
		TaskResult taskResult = new TaskResult();
		try {
			if (MapUtils.isNotEmpty(taskData)) {
				String uri = (String) taskData.get("uri");
				if (StringUtils.isBlank(uri)) {
					taskResult.setFailedResult("Mongodb uri must not be null.");
					callback.accept(taskResult);
					return;
				}

				String collection = (String) taskData.get("collection");
				if (StringUtils.isBlank(collection)) {
					taskResult.setFailedResult("Collection name must not be null.");
					callback.accept(taskResult);
					return;
				}

				String database = MongodbUtil.getDatabase(uri);
				if (StringUtils.isBlank(database)) {
					taskResult.setFailedResult("Mongodb uri does not has database.");
					callback.accept(taskResult);
					return;
				}
				MongoClient mongoClient = MongodbUtil.createMongoClient(uri);

				String validationLevel = (String) taskData.get("validationLevel");
				if (StringUtils.isBlank(validationLevel)) {
					taskResult.setFailedResult("Must be set validation level.");
					callback.accept(taskResult);
					return;
				}

				List rules = (List) taskData.get("rules");
				Document validator = mongodbValidator(rules);

				MongoDatabase mongoClientDatabase = mongoClient.getDatabase(database);

				boolean collExists = false;
				MongoIterable<String> collectionNames = mongoClientDatabase.listCollectionNames();
				for (String collectionName : collectionNames) {
					if (collectionName.equals(collection)) {
						collExists = true;
						break;
					}
				}
				if (!collExists) {
					mongoClientDatabase.createCollection(collection);
				}

				mongoClientDatabase.runCommand(
						new Document("collMod", collection)
								.append("validator", validator)
								.append("validationLevel", validationLevel)
				);

				taskResult.setPassResult();
			} else {
				taskResult.setTaskResult("Task data must not be null.");
			}
		} catch (Exception e) {
			taskResult.setFailedResult(e.getMessage());
		}
		callback.accept(taskResult);
	}

	public Document mongodbValidator(List<Map<String, Object>> rules) {

		Document validator = new Document();
		if (CollectionUtils.isNotEmpty(rules)) {
//            validator.put("$and", new ArrayList<>());
			for (Map<String, Object> fieldRule : rules) {
				String fieldName = (String) fieldRule.get("field_name");
				Boolean nullable = fieldRule.get("nullable") == null ? false : (Boolean) fieldRule.get("nullable");
				Map<String, Object> ruleDef = (Map<String, Object>) fieldRule.get("rule_def");
				if (StringUtils.isNotBlank(fieldName) &&
						MapUtils.isNotEmpty(ruleDef)) {
					String rule = (String) ruleDef.get("rules");
					if (StringUtils.isNotBlank(rule)) {
						Document doc = Document.parse(rule);

						if (MapUtils.isNotEmpty(doc)) {
							Map<String, Object> result = new HashMap<>();
							for (Map.Entry<String, Object> entry : doc.entrySet()) {
								String ruleType = entry.getKey();
								Object condition = entry.getValue();

								switch (DataRules.RuleType.fromString(ruleType)) {
									case EXISTS:
										result = exists(fieldName, condition);
										break;
									case TYPE:
										result = type(fieldName, condition);
										break;
									case RANGE:
										result = range(fieldName, condition);
										break;
									case ENUM:
										result = enumChk(fieldName, condition);
										break;
									case REGEX:
										result = regex(fieldName, condition);
										break;
									default:
										break;
								}

							}

							if (nullable != null && nullable) {
								result = nullable(fieldName, result);
							}

							if (validator.containsKey("$and")) {
								((List) validator.get("$and")).add(result);
							} else {
								if (validator.containsKey("$or") && result.containsKey("$or")) {
									ArrayList<Object> andCondition = new ArrayList<>();
									for (Map.Entry<String, Object> entry : validator.entrySet()) {
										andCondition.add(new Document(entry.getKey(), entry.getValue()));
									}
									andCondition.add(result);
									validator.clear();
									validator.put("$and", andCondition);
								} else {
									validator.putAll(result);
								}
							}
						}
					}
				}
			}
		}

		return validator;
	}

	private Map<String, Object> regex(String fieldName, Object condition) {
		Map<String, Object> result = new HashMap<>();
		String con = null;
		try {
			con = (String) condition;
		} catch (Exception e) {
			return result;
		}

		if (StringUtils.isEmpty(con)) {
			return result;
		}

		Map<String, Object> typeCondition = new HashMap<>(1);
		typeCondition.put("$regex", new BsonRegularExpression(con));

		result.put(fieldName, typeCondition);

		return result;
	}

	private Map<String, Object> enumChk(String fieldName, Object condition) {
		Map<String, Object> result = new HashMap<>();
		List<Object> con;
		try {
			con = (List<Object>) condition;
		} catch (Exception e) {
			return result;
		}

		if (CollectionUtils.isEmpty(con)) return result;

		Map<String, Object> inCondition = new HashMap<>(1);
		inCondition.put("$in", condition);
		result.put(fieldName, inCondition);

		return result;
	}

	private Map<String, Object> range(String fieldName, Object condition) {
		Map<String, Object> result = new HashMap<>();

		Map<String, Object> con = null;
		try {
			con = (Map<String, Object>) condition;
		} catch (Exception e) {
			return result;
		}

		if (MapUtils.isEmpty(con)) {
			return result;
		}

		Map<String, Object> rangeCondition = new HashMap<>(2);
		for (Map.Entry<String, Object> entry : con.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if (value instanceof String) {
				BigDecimal bigDecimal = new BigDecimal((String) value);
				value = bigDecimal.doubleValue();
			}
			switch (key) {
				case "gt":
					rangeCondition.put("$gt", value);
					break;
				case "gte":
					rangeCondition.put("$gte", value);
					break;
				case "lt":
					rangeCondition.put("$lt", value);
					break;
				case "lte":
					rangeCondition.put("$lte", value);
					break;
			}
		}

		if (MapUtils.isNotEmpty(rangeCondition)) {
			result.put(fieldName, rangeCondition);
		}
		return result;
	}

	private Map<String, Object> type(String fieldName, Object condition) {
		Map<String, Object> result = new HashMap<>();
		String con = null;
		try {
			con = (String) condition;
		} catch (Exception e) {
			return result;
		}

		if (StringUtils.isEmpty(con)) {
			return result;
		}

		Map<String, Object> typeCondition = new HashMap<>(1);
		typeCondition.put("$type", con);

		result.put(fieldName, typeCondition);

		return result;
	}

	private Map<String, Object> nullable(String fieldName, Map<String, Object> condition) {
		Map<String, Object> result = new HashMap<>();

		List<Map<String, Object>> nullableCondition = new ArrayList<>();

		nullableCondition.add(new Document(fieldName, new Document("$exists", false)));
		nullableCondition.add(new Document(fieldName, null));
		if (MapUtils.isNotEmpty(condition)) {
			nullableCondition.add(condition);
		}

		result.put("$or", nullableCondition);

		return result;
	}

	private Map<String, Object> exists(String fieldName, Object condition) {
		Map<String, Object> result = new HashMap<>();
		Map<String, Object> con = null;
		try {
			con = (Map<String, Object>) condition;
		} catch (Exception e) {
			return result;
		}

		if (MapUtils.isEmpty(con)) {
			return result;
		}

		result.put(fieldName, con);

		return result;
	}

}
