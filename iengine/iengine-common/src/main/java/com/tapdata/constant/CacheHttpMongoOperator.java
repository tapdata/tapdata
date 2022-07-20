package com.tapdata.constant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.tapdata.entity.Job;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.mongo.RestTemplateOperator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class CacheHttpMongoOperator extends HttpClientMongoOperator {

	private final ConcurrentMap<String, String> cache;

	private Logger logger = LogManager.getLogger(getClass());

	private ScheduledExecutorService service = Executors.newScheduledThreadPool(1);

	public CacheHttpMongoOperator(MongoTemplate template, MongoClient mongoClient, RestTemplateOperator restTemplateOperator, ConfigurationCenter configCenter, MongoClientURI mongoClientURI) {
		super(template, mongoClient, restTemplateOperator, configCenter);

		this.cache = CacheBuilder.newBuilder()
				.expireAfterWrite(300, TimeUnit.SECONDS)
				.build(new CacheLoader<String, String>() {
					public String load(String jobId) throws Exception {
						if (StringUtils.isBlank(jobId)) {
							return null;
						}
						Query query = new Query(where("id").is(jobId));
						List<Job> jobs = CacheHttpMongoOperator.super.find(query, ConnectorConstant.JOB_COLLECTION, Job.class);
						if (CollectionUtils.isNotEmpty(jobs)) {
							try {
								return JSONUtil.obj2Json(jobs.get(0));
							} catch (JsonProcessingException e) {
								logger.error("CacheHttpMongoOperator convert job entity {} to json failed {}", jobs.get(0), e.getMessage());
							}
						}

						return null;
					}
				}).asMap();

		service.scheduleWithFixedDelay(() -> {

			if (MapUtils.isNotEmpty(cache)) {
				try {
					Set<String> jobids = cache.keySet();

					Query query = new Query(where("_id").is(new Document("inq", jobids)));

					List<Job> jobs = super.find(query, ConnectorConstant.JOB_COLLECTION, Job.class);
					if (CollectionUtils.isNotEmpty(jobs)) {
						for (Job job : jobs) {
							cache.put(job.getId(), JSONUtil.obj2Json(job));
						}
					}
				} catch (Exception e) {
					logger.error("CacheHttpMongoOperator refresh job cache failed {}", e.getMessage(), e);
				}
			}

		}, 10, 2, TimeUnit.SECONDS);

	}

	@Override
	public <T> List<T> find(Query query, String collection, Class<T> className) {

//        if (ConnectorConstant.JOB_COLLECTION.equals(collection) && needToCache(query)) {
//            List<T> result = new ArrayList<>(1);
//            Document queryObject = query.getQueryObject();
//            Object jobId = queryObject.get("_id") == null ? queryObject.get("id") : queryObject.get("_id");
//            if (jobId != null) {
//                String jobIdStr = null;
//                if (jobId instanceof String) {
//                    jobIdStr = (String) jobId;
//                } else {
//                    jobIdStr = ((ObjectId) jobId).toHexString();
//                }
//
//                String job = cache.get(jobIdStr);
//                if (StringUtils.isNotBlank(job)) {
//                    try {
//                        result.add(JSONUtil.json2POJO(job, className));
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//            }
//            return result;
//        }
		return super.find(query, collection, className);
	}

	private boolean needToCache(Query query) {

		if (query == null) {
			return false;
		}

		Document queryObject = query.getQueryObject();

		return (queryObject.size() == 1) && (queryObject.containsKey("_id") || queryObject.containsKey("id"));
	}
}
