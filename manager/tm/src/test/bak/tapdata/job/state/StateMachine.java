/**
 * @title: StateMachine
 * @description:
 * @author lk
 * @date 2021/7/30
 */
package com.tapdata.job.state;


import java.util.Map;
import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

@Component
public class StateMachine {

	@Autowired
	MongoTemplate mongoTemplate;

	private static StateMachineExecutor stateMachineExecutor = new StateMachineExecutor();


	public void handleEvent(String id, Event event){
		Map map = mongoTemplate.findOne(Query.query(Criteria.where("jobId").is(id)), Map.class, "jobtest");
		if (MapUtils.isEmpty(map)){
			System.out.println("Job info does not exist");
			return;
		}
		String status = map.get("status").toString();
		State state = State.getState(status);
		if (state == null){
			System.out.println(String.format("Job state %s is illegal", status));
			return;
		}
		StateMachineContext stateMachineContext = new StateMachineContext();
		stateMachineContext.setSource(state);
		stateMachineContext.setMongoTemplate(mongoTemplate);
		stateMachineContext.setEvent(event);
		stateMachineExecutor.handleEvent(stateMachineContext);
	}
}
