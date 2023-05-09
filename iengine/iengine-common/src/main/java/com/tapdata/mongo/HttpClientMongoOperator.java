package com.tapdata.mongo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.DateUtil;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.Job;
import com.tapdata.entity.LoginResp;
import com.tapdata.entity.ResponseCount;
import com.tapdata.entity.ScheduleTask;
import com.tapdata.entity.User;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.exception.ManagementException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class HttpClientMongoOperator extends ClientMongoOperator {

	private Logger logger = LogManager.getLogger(getClass());

	private MongoTemplate mongoTemplate;

	private RestTemplateOperator restTemplateOperator;

	private ConfigurationCenter configCenter;

	private final static int EXCLUDE_MODE = 0;
	private final static int INCLUDE_MODE = 1;
	private final static int ALL_MODE = 2;

	public HttpClientMongoOperator(MongoTemplate template, MongoClient mongoClient, RestTemplateOperator restTemplateOperator, ConfigurationCenter configCenter) {

		super(template, mongoClient);

		this.mongoTemplate = template;

		this.restTemplateOperator = restTemplateOperator;

		this.configCenter = configCenter;
	}

	public HttpClientMongoOperator(MongoTemplate template, MongoClient mongoClient, MongoClientURI mongoClientURI, RestTemplateOperator restTemplateOperator, ConfigurationCenter configCenter) {

		super(template, mongoClient, mongoClientURI);

		this.mongoTemplate = template;

		this.restTemplateOperator = restTemplateOperator;

		this.configCenter = configCenter;
	}

	@Override
	public MongoTemplate getMongoTemplate() {
		return mongoTemplate;
	}

	@Override
	public void delete(Map<String, Object> params, String collection) {

		validateToken();

		Query query = new Query(getAndCriteria(params));

		delete(query, collection);
	}

	@Override
	public void delete(Query query, String collection) {
		validateToken();

		List<Map> maps = find(query, collection, Map.class);

		StringBuilder sb = new StringBuilder(collection).append("/");
		if (CollectionUtils.isNotEmpty(maps)) {

			for (Map map : maps) {
				if (map.containsKey("id")) {
					Object id = map.get("id");

					restTemplateOperator.deleteById(addToken(sb.append(id).toString()), id);
				}
			}
		}
	}

	@Override
	public void deleteByMap(Map<String, Object> params, String collection) {
		validateToken();
		restTemplateOperator.delete(addToken(collection), params);
	}

	/**
	 * 这个不能随意调用，需要预先在backend项目新增自定义deleteAll接口
	 *
	 * @param params
	 * @param collection
	 */
	@Override
	public void deleteAll(Map<String, Object> params, String collection) {
		validateToken();

		Map<String, Object> where = new HashMap<>();
		if (MapUtils.isNotEmpty(params)) {
			where = new HashMap<String, Object>() {{
				try {
					put("where", JSONUtil.obj2Json(params));
				} catch (JsonProcessingException e) {
					throw new RuntimeException("Convert params map to json string failed; map: " + params + "; " + e.getMessage());
				}
			}};
		}

		addToken(where);
		restTemplateOperator.post(where, collection + "/deleteAll", where);
	}

	@Override
	public void upsert(Map<String, Object> params, Map<String, Object> insert, String collection) {
		upsert(params, insert, collection, Map.class);
	}

	@Override
	public <T> T upsert(Map<String, Object> params, Map<String, Object> insert, String collection, Class<T> clazz) {
		validateToken();
		Query query = new Query(getAndCriteria(params));
		Update update = getUpdate(insert);

		Document queryObject = query.getQueryObject();
		Map<String, Object> reqParams = new HashMap<>();
		addToken(reqParams);

		if (MapUtils.isNotEmpty(queryObject)) {
			reqParams.put("where", queryObject.toJson());
		}

		try {
			T t = restTemplateOperator.upsert(reqParams, update.getUpdateObject().get("$set"), collection, clazz);
			return t;
		} catch (Exception e) {
			throw new ManagementException(e.getMessage(), e);
		}
	}

	@Override
	public <T> T findAndModify(Map<String, Object> params, Map<String, Object> updateParams, Class<T> className, String collection) {

		validateToken();

		Query query = new Query(getAndCriteria(params));
		Update update = getUpdate(updateParams);

		T result = findAndModifyByCollection(collection, query, update, true);
		return result;
	}

	@Override
	public <T> T findAndModify(Query query, Update update, Class<T> className, String collection, boolean returnNew) {

		validateToken();

		T result = findAndModifyByCollection(collection, query, update, returnNew);

		return result;
	}

	@Override
	public <T> T findAndModify(Query query, Update update, Class<T> className, String collection) {

		validateToken();

		T result = findAndModifyByCollection(collection, query, update, true);

		return result;
	}

	@Override
	public UpdateResult updateAndParam(Map<String, Object> params, Map<String, Object> updateParams, String collection) {

		validateToken();

		Query query = new Query(getAndCriteria(params));
		Update update = getUpdate(updateParams);
		UpdateResult result = update(query, update, collection);

		return result;
	}


	@Override
	public <T> List<T> find(Map<String, Object> params, String collection, Class<T> className) {

		validateToken();

		Query query = new Query(getAndCriteria(params));
		return find(query, collection, className);
	}

	@Override
	public UpdateResult update(Query query, Update update, String collection) {

		validateToken();

		Document queryObject = query.getQueryObject();
		Map<String, Object> params = new HashMap<>();
		addToken(params);

		if (MapUtils.isNotEmpty(queryObject)) {
			params.put("where", queryObject.toJson());
		}


		UpdateResult result = null;
		try {
			Map<String, Object> postResult = restTemplateOperator.post(params, update.getUpdateObject(), collection, Map.class);

			if (MapUtils.isNotEmpty(postResult)) {
				result = UpdateResult.acknowledged(Long.valueOf(String.valueOf(postResult.getOrDefault("count", 0))), Long.valueOf(String.valueOf(postResult.getOrDefault("count", 0))), null);
			}
		} catch (Exception e) {
			throw new ManagementException(e.getMessage(), e);
		}


		return result;
	}

	@Override
	public <T> T updateById(Update update, String collection, String id, Class<T> className) {

		validateToken();

		final String resource = addToken(collection + "/" + id);

		T result = null;
		try {
			result = restTemplateOperator.postOne(update.getUpdateObject(), resource, className);

		} catch (Exception e) {
			throw new ManagementException(e.getMessage(), e);
		}

		return result;
	}

	@Override
	public <T> List<T> find(Query query, String collection, Class<T> className) {

		validateToken();

		Map<String, Object> params = new HashMap<>();

		handleParams(query, params, className);

		List list;
		try {
			list = restTemplateOperator.getBatch(params, collection, className, cookies(), cloudRegion);
		} catch (Exception e) {
			throw new ManagementException(e.getMessage(), e);
		}
		return list == null ? new ArrayList<>() : list;
	}

	@Override
	public <T> List<T> find(Query query, String collection, Class<T> className, Predicate<?> stop) {

		validateToken();

		Map<String, Object> params = new HashMap<>();

		handleParams(query, params, className);

		List list;
		try {
			list = restTemplateOperator.getBatch(params, collection, className, cookies(), cloudRegion, stop);
		} catch (Exception e) {
			throw new ManagementException(e.getMessage(), e);
		}
		return list == null ? new ArrayList<>() : list;
	}

	@Override
	public <T> T findOne(Query query, String collection, Class<T> className) {

		validateToken();

		Map<String, Object> params = new HashMap<>();

		handleParams(query, params, className);

		T result;
		try {
			result = restTemplateOperator.getOne(params, collection, className, cookies(), cloudRegion);
		} catch (Exception e) {
			throw new ManagementException(e.getMessage(), e);
		}
		return result;
	}

	@Override
	public <T> T findOne(Query query, String collection, Class<T> className, Predicate<?> stop) {

		validateToken();

		Map<String, Object> params = new HashMap<>();

		handleParams(query, params, className);

		T result;
		try {
			result = restTemplateOperator.getOne(params, collection, className, cookies(), cloudRegion, stop);
		} catch (Exception e) {
			throw new ManagementException(e.getMessage(), e);
		}
		return result;
	}

	@Override
	public <T> T findOne(Map<String, Object> params, String collection, Class<T> className) {

		validateToken();
		addToken(params);
		T result;
		try {
			result = restTemplateOperator.getOne(params, collection, className, cookies(), cloudRegion);
		} catch (Exception e) {
			throw new ManagementException(e.getMessage(), e);
		}
		return result;
	}

	@Override
	public long count(Query query, String collection) {

		validateToken();

		Map<String, Object> params = new HashMap<>();
		addToken(params);
		Document queryObject = query.getQueryObject();
		if (MapUtils.isNotEmpty(queryObject)) {
			queryObject.keySet().forEach(key -> {
				if (queryObject.get(key) instanceof Document) {
					Document subDocument = ((Document) queryObject.get(key));
					if (subDocument.containsKey("$in")) {
						subDocument.put("inq", subDocument.get("$in"));
						subDocument.remove("$in");
					}
				}
			});
			params.put("where", queryObject.toJson());
		}

		ResponseCount responseCount = restTemplateOperator.getOne(params, collection + "/count", ResponseCount.class, cookies(), cloudRegion);
		if (responseCount == null) {
			return 0;
		} else {
			return responseCount.getCount();
		}
	}

	/**
	 * 调用时需要注意管理端是否支持customCount自定义函数
	 *
	 * @param query
	 * @param collection
	 * @return
	 */
	@Override
	public long postCount(Query query, String collection) {
		validateToken();

		Map<String, Object> params = new HashMap<>();
		Document queryObject = query.getQueryObject();
		queryObject.forEach((k, v) -> params.put(k, v));
		String resource = collection + "/customCount";

		ResponseCount responseCount = restTemplateOperator.postOne(params, addToken(resource), ResponseCount.class);
		if (responseCount != null) {
			return responseCount.getCount();
		} else {
			return 0;
		}
	}

	@Override
	public long count(Query query, String collection, Class clazz) {

		validateToken();

		query.fields().include("name");
		List list = find(query, collection, clazz);
		return list.size();
	}

	@Override
	public void pullObjectToArray(Map<String, Object> params, Map<String, Object> updateParams, String collection) {

		validateToken();

		Query query = new Query(getAndCriteria(params));
		Update update = new Update();
		updateParams.forEach((key, value) -> {
			update.push(key, value);
		});
		mongoTemplate.updateFirst(query, update, collection);
	}

	@Override
	public void createIndexes(String collection, List<IndexModel> indexModels) {

		validateToken();

		MongoCollection<Document> mongoCollection = mongoTemplate.getCollection(collection);
		mongoCollection.createIndexes(indexModels);
	}

	@Override
	public void insertOne(Object obj, String collection) {

		validateToken();

		Map<String, Object> params = new HashMap<>();
		addToken(params);
		restTemplateOperator.post(obj, collection, params);

	}

	@Override
	public void insertList(List<? extends Object> list, String collection) {

		validateToken();

		for (Object o : list) {
			insertOne(o, collection);
		}
	}

	@Override
	public void insertMany(List<?> list, String collection) {
		validateToken();

		Map<String, Object> params = new HashMap<>();
		addToken(params);
		restTemplateOperator.post(list, collection, params);
	}

	@Override
	public void insertMany(List<?> list, String collection, Predicate<?> stop) {
		validateToken();

		Map<String, Object> params = new HashMap<>();
		addToken(params);
		restTemplateOperator.post(list, collection, params, stop);
	}

	@Override
	public void batch(List<?> list, String collection, Predicate<?> stop) {
		validateToken();

		Map<String, Object> params = new HashMap<>();
		addToken(params);
		restTemplateOperator.post(list, collection + "/batch", params, stop);
	}

	@Override
	public void releaseResource() {
		if (mongoTemplate != null) {
			Mongo mongo = mongoTemplate.getMongoDbFactory().getLegacyDb().getMongo();
			mongo.close();
		}
	}

	public Map<String, Object> addToken(Map<String, Object> params) {
		params.put("access_token", configCenter.getConfig(ConfigurationCenter.TOKEN));
		return params;
	}

	public String addToken(String url) {
		StringBuilder sb = new StringBuilder(url);

		sb.append("?").append("access_token=").append(configCenter.getConfig(ConfigurationCenter.TOKEN));

		return sb.toString();
	}

	public String cookies() {
		StringBuilder sb = new StringBuilder();

		User user = (User) configCenter.getConfig(ConfigurationCenter.USER_INFO);

		if (user != null) {
			Integer role = user.getRole();
			sb.append("isAdmin=").append(role).append(";user_id=").append(user.getId());
		}

		return sb.toString();
	}

	public Job findAndModifyJob(String collection, Query query, Update update, boolean returnNew) {
		if (ConnectorConstant.JOB_COLLECTION.equals(collection)) {
			final org.springframework.data.mongodb.core.query.Field fields = query.fields();
			if (fields == null || MapUtils.isEmpty(fields.getFieldsObject())) {
				fields.exclude("editorData");
			}
			if (query.getLimit() <= 0) {
				query.limit(1);
			}
			List<Job> jobs = find(query, collection, Job.class);

			if (CollectionUtils.isNotEmpty(jobs)) {

				for (Job job : jobs) {
					String jobId = job.getId();

					if (!query.getQueryObject().containsKey("_id")) {
						query.addCriteria(new Criteria().and("_id").is(jobId));
					}
					UpdateResult result = update(query, update, collection);
					if (result.getModifiedCount() > 0) {

						if (returnNew) {

							Query newRecordQuery = new Query(where("_id").is(jobId));
							newRecordQuery.fields().exclude("editorData");
							jobs = find(newRecordQuery, collection, Job.class);

							if (CollectionUtils.isNotEmpty(jobs)) {
								return jobs.get(0);
							}
						} else {
							return job;
						}
					}

					break;
				}
			}
		}

		return null;
	}

	public ScheduleTask findAndModifTask(String collection, Query query, Update update, boolean returnNew) {
		if (ConnectorConstant.SCHEDULE_TASK_COLLECTION.equals(collection)) {
			List<ScheduleTask> scheduleTasks = find(query, collection, ScheduleTask.class);

			if (CollectionUtils.isNotEmpty(scheduleTasks)) {

				for (ScheduleTask scheduleTask : scheduleTasks) {
					String taskId = scheduleTask.getId();

					query.addCriteria(new Criteria().and("_id").is(taskId));
					UpdateResult result = update(query, update, collection);
					if (result.getModifiedCount() > 0) {

						if (returnNew) {

							scheduleTasks = find(new Query(where("_id").is(taskId)), collection, ScheduleTask.class);

							if (CollectionUtils.isNotEmpty(scheduleTasks)) {
								return scheduleTasks.get(0);
							}
						} else {
							return scheduleTask;
						}
					}
					break;
				}
			}
		}

		return null;
	}

	public File downloadFile(Map<String, Object> params, String resource, String filePath, boolean replace) {
		if (!replace) {
			File file = new File(filePath);
			if (file.exists()) {
				return file;
			}
		}

		if (params == null) {
			params = new HashMap<>();
		}
		addToken(params);

		return restTemplateOperator.downloadFile(params, resource, filePath, cookies(), cloudRegion);
	}

	public RestTemplateOperator getRestTemplateOperator() {
		return restTemplateOperator;
	}

	public ConfigurationCenter getConfigCenter() {
		return configCenter;
	}

	private Criteria getAndCriteria(Map<String, Object> queryParam) {
		Criteria criteria = new Criteria();
		if (queryParam != null && !queryParam.isEmpty()) {
			queryParam.forEach((s, o) -> criteria.and(s).is(o));
		}
		return criteria;
	}

	private Update getUpdate(Map<String, Object> insert) {
		Update update = new Update();
		if (insert != null && !insert.isEmpty()) {
			insert.forEach((key, value) -> {
				if (("id".equals(key) || "_id".equals(key)) && value == null) {
					return;
				}
				update.set(key, value);
			});
		}
		return update;
	}

	private Object findAndModifyDataFlow(String collection, Query query, Update update, boolean returnNew) {
		if (ConnectorConstant.DATA_FLOW_COLLECTION.equals(collection)) {
			final org.springframework.data.mongodb.core.query.Field fields = query.fields();
			if (fields == null || MapUtils.isEmpty(fields.getFieldsObject())) {
				fields.exclude("editorData");
			}
			if (query.getLimit() <= 0) {
				query.limit(1);
			}
			List<DataFlow> dataFlows = find(query, collection, DataFlow.class);

			if (CollectionUtils.isNotEmpty(dataFlows)) {

				for (DataFlow dataFlow : dataFlows) {
					String dataFlowId = dataFlow.getId();

					query.addCriteria(new Criteria().and("_id").is(dataFlowId));
					UpdateResult result = update(query, update, collection);
					if (result.getModifiedCount() > 0) {

						if (returnNew) {

							Query newRecordQuery = new Query(where("_id").is(dataFlowId));
							newRecordQuery.fields().exclude("graph");
							dataFlows = find(newRecordQuery, collection, DataFlow.class);

							if (CollectionUtils.isNotEmpty(dataFlows)) {
								return dataFlows.get(0);
							}
						} else {
							return dataFlow;
						}
					}
					break;
				}
			}
		}

		return null;
	}

	private Object findAndModifyTask(String collection, Query query, Update update, boolean returnNew) {
		if (ConnectorConstant.TASK_COLLECTION.equals(collection)) {
			final org.springframework.data.mongodb.core.query.Field fields = query.fields();
			if (fields == null || MapUtils.isEmpty(fields.getFieldsObject())) {
				fields.exclude("editorData");
			}
			if (query.getLimit() <= 0) {
				query.limit(1);
			}
			List<TaskDto> taskDtos = find(query, collection, TaskDto.class);

			if (CollectionUtils.isNotEmpty(taskDtos)) {

				for (TaskDto taskDto : taskDtos) {
					String taskId = taskDto.getId().toHexString();

					query.addCriteria(new Criteria().and("_id").is(taskId));
					UpdateResult result = update(query, update, collection);
					if (result.getModifiedCount() > 0) {

						if (returnNew) {

							Query newRecordQuery = new Query(where("_id").is(taskId));
							newRecordQuery.fields().exclude("graph");
							final TaskDto newTaskDto = findOne(newRecordQuery, collection, TaskDto.class);

							if (newTaskDto != null) {
								return newTaskDto;
							}
						} else {
							return taskDto;
						}
					}
					break;
				}
			}
		}

		return null;
	}

	private <T> T findAndModifyByCollection(String collection, Query query, Update update, boolean returnNew) {
		T result = null;
		if (ConnectorConstant.SCHEDULE_TASK_COLLECTION.equals(collection)) {
			result = (T) findAndModifTask(collection, query, update, returnNew);
		} else if (ConnectorConstant.JOB_COLLECTION.equals(collection)) {
			result = (T) findAndModifyJob(collection, query, update, returnNew);
		} else if (ConnectorConstant.DATA_FLOW_COLLECTION.equals(collection)) {
			result = (T) findAndModifyDataFlow(collection, query, update, returnNew);
		} else if (ConnectorConstant.TASK_COLLECTION.equals(collection)) {
			result = (T) findAndModifyTask(collection, query, update, returnNew);
		}
		return result;
	}

	private void validateToken() {

		LoginResp loginResp = (LoginResp) configCenter.getConfig(ConfigurationCenter.LOGIN_INFO);

		if (loginResp == null) {
			refreshToken();
		} else {
			long expiredTimestamp = loginResp.getExpiredTimestamp();
			if (expiredTimestamp - System.currentTimeMillis() <= 86400 * 1000) {
				refreshToken();
			}
		}
	}

	private void refreshToken() {
		String accessCode = (String) configCenter.getConfig(ConfigurationCenter.ACCESS_CODE);

		LoginResp loginResp = null;
		Map<String, Object> params = new HashMap<>();
		params.put("accesscode", accessCode);

		int retryCount = 0;
		while (loginResp == null && retryCount <= 5) {
			try {
				loginResp = restTemplateOperator.postOne(params, "users/generatetoken", LoginResp.class);
				if (loginResp != null) {
					Date date = (Date) DateUtil.parse(loginResp.getCreated());
					long expiredTimestamp = date.getTime() + (loginResp.getTtl() * 1000);
					loginResp.setExpiredTimestamp(expiredTimestamp);

					configCenter.putConfig(ConfigurationCenter.TOKEN, loginResp.getId());
					configCenter.putConfig(ConfigurationCenter.USER_ID, loginResp.getUserId());
					configCenter.putConfig(ConfigurationCenter.LOGIN_INFO, loginResp);
					StringBuilder sb = new StringBuilder("users").append("/").append(loginResp.getUserId());
					User user = findOne(new Query(), sb.toString(), User.class);
					configCenter.putConfig(ConfigurationCenter.USER_INFO, user);

					retryCount++;
				} else {
					logger.warn("Login fail response {}, waiting 60(s) retry.", loginResp);
					try {
						Thread.sleep(60000L);
					} catch (InterruptedException e) {

					}
				}
			} catch (Exception e) {
				logger.error("refresh token failed {}", e.getMessage(), e);
			}
		}
	}

	@Override
	public <T> T postOne(Map<String, Object> obj, String resource, Class<T> className) {
		validateToken();
		if (null == obj) {
			obj = new HashMap<>();
		}
		return restTemplateOperator.postOne(obj, addToken(resource), className);
	}

	private void appendProperty(Document doc, String[] keys, Object value, int length, int count) {
		if (count >= length - 1) {
			doc.append(keys[count], value);
			return;
		}
		Document childDoc = new Document();
		doc.append(keys[count], childDoc);
		appendProperty(childDoc, keys, value, length, ++count);
	}

	private <T> void handleParams(Query query, Map<String, Object> params, Class<T> className) {
		if (query == null) {
			return;
		}
		Document queryObject = query.getQueryObject();
		addToken(params);

		Document filterDocument = new Document();
		if (MapUtils.isNotEmpty(queryObject)) {
			queryObject.keySet().forEach(key -> {
				if (queryObject.get(key) instanceof Document) {
					Document subDocument = ((Document) queryObject.get(key));
					if (subDocument.containsKey("$in")) {
						subDocument.put("inq", subDocument.get("$in"));
						subDocument.remove("$in");
					}
				}
			});
			filterDocument.append("where", queryObject);
		}

//    Optional.ofNullable(className).ifPresent(clazz -> handleFieldFilter(query, filterDocument, className));

		Document sortObject = query.getSortObject();
		if (MapUtils.isNotEmpty(sortObject)) {
			List<String> orders = new ArrayList<>();
			for (Map.Entry<String, Object> entry : sortObject.entrySet()) {
				String key = entry.getKey();
				Object value = entry.getValue();
				String order = "DESC";
				if (value instanceof Integer && ((int) value) > 0) {
					order = "ASC";
				}
				StringBuilder sb = new StringBuilder();
				sb.append(key).append(" ").append(order);
				orders.add(sb.toString());
			}

			filterDocument.append("order", orders);
		}

		int limit = query.getLimit();
		if (limit > 0) {
			filterDocument.append("limit", limit);
		}

		long skip = query.getSkip();
		if (skip > 0) {
			filterDocument.append("skip", (int) skip);
		}

		if (MapUtils.isNotEmpty(filterDocument)) {
			params.put("filter", filterDocument.toJson());
		}
	}

	private <T> void handleFieldFilter(Query query, Document filterDocument, Class<T> className) {
		Document fieldsObject = query.getFieldsObject();
		Document fields = new Document();

		int mode = ALL_MODE;
		if (MapUtils.isNotEmpty(fieldsObject)) {
			for (Map.Entry<String, Object> entry : fieldsObject.entrySet()) {
				mode = (int) entry.getValue();
				break;
			}
		}

		Field[] declaredFields = className.getDeclaredFields();
		if (ArrayUtils.isNotEmpty(declaredFields)) {
			// 包含所有父类属性
			List<Field> allFields = new ArrayList<>();
			Collections.addAll(allFields, declaredFields);
			Class superClz = className.getSuperclass();
			while (null != superClz) {
				Field[] superFields = superClz.getDeclaredFields();
				Collections.addAll(allFields, superFields);
				superClz = superClz.getSuperclass();
			}

			for (java.lang.reflect.Field f : allFields) {
				String fieldName = f.getName();
				if (!Modifier.isStatic(f.getModifiers())) {

					if (mode == ALL_MODE) {
						fields.append(fieldName, true);
					} else if (mode == INCLUDE_MODE) {
						if (fieldsObject.containsKey(fieldName) && (int) fieldsObject.get(fieldName) == 1) {
							fields.append(fieldName, true);
						}
					} else if (mode == EXCLUDE_MODE) {
						if (!fieldsObject.containsKey(fieldName) || (fieldsObject.containsKey(fieldName) && (int) fieldsObject.get(fieldName) != 0)) {
							fields.append(fieldName, true);
						}
					}

				}
			}
		} else if (MapUtils.isNotEmpty(fieldsObject)) {
			for (Map.Entry<String, Object> entry : fieldsObject.entrySet()) {
				if (mode == INCLUDE_MODE) {
					fields.append(entry.getKey(), true);
				} else {
					fields.append(entry.getKey(), false);
				}
			}

		}

		if (MapUtils.isNotEmpty(fields)) {
			filterDocument.append("fields", fields);
		}
	}

	public static void main(String[] args) {
		Query query = new Query();
		query.with(Sort.by(Sort.Order.desc("createTime"), Sort.Order.asc("updateTime")));

		Document sortObject = query.getSortObject();
		System.out.println(sortObject);
	}
}
