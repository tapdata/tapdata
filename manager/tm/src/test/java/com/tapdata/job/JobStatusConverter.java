/**
 * @title: JobStatusConverter
 * @description:
 * @author lk
 * @date 2021/7/19
 */
package com.tapdata.job;

import com.tapdata.tm.base.exception.BizException;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

@Component
public class JobStatusConverter {

	@Autowired
	MongoTemplate mongoTemplate;

	public void handle(String id, String target) throws Exception {
		JobStatus status = JobStatus.getStatus(target);
		System.out.println("id: " + id + ",target: " + target);
		if (status == null || CollectionUtils.isEmpty(status.getSource())){
			System.out.println("Job status is null...");
			throw new BizException(String.format("Status %s not exist", target));
		}

		Map map = mongoTemplate.findOne(Query.query(Criteria.where("jobId").is(id)), Map.class, "jobtest");
		String source = MapUtils.isEmpty(map) ? null : map.get("status").toString();
		System.out.println("source: " + source);
		if (!status.getSource().contains(source)){
			System.out.println("Job status not contains...");
			throw new BizException(String.format("Status change from %s to %s is not allowed", source, target));
		}
		JobStatusProcessor processor = JobStatusUtils.getProcessor(target);
		processor.handle(id, source, target, mongoTemplate);
	}
}
