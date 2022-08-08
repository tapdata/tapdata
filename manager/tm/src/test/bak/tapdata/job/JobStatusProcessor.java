/**
 * @title: JobStatusAbsService
 * @description:
 * @author lk
 * @date 2021/7/19
 */
package com.tapdata.job;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.utils.JsonUtil;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

public abstract class JobStatusProcessor {

//	@Autowired
//	private MongoTemplate mongoTemplate;

	abstract void beforeHandle(String source, String target);

	void handle(String id, String source, String target, MongoTemplate mongoTemplate){
		beforeHandle(source, target);
//		Map map = mongoTemplate.findAndModify(Query.query(Criteria.where("jobId").is(id).and("status").is(source)), Update.update("status", target), Map.class, "jobtest");
		UpdateResult updateResult = mongoTemplate.updateFirst(Query.query(Criteria.where("jobId").is(id).and("status").is(source)), Update.update("status", target), "jobtest");
		if (updateResult.wasAcknowledged() && updateResult.getModifiedCount() > 0){
			System.out.println("result: " + JsonUtil.toJson(updateResult));
			afterHandle(source, target);
		}

	}

	abstract void afterHandle(String source, String target);
}
