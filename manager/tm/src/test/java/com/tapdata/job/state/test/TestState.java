/**
 * @title: TestState
 * @description:
 * @author lk
 * @date 2021/7/30
 */
package com.tapdata.job.state.test;

import com.tapdata.job.state.Event;
import com.tapdata.job.state.State;
import com.tapdata.job.state.StateMachine;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.powermock.api.mockito.PowerMockito.when;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

@RunWith(PowerMockRunner.class)
public class TestState {

	@Mock
	MongoTemplate mongoTemplate;

	@InjectMocks
	StateMachine stateMachine = new StateMachine();

	@Test
	public void test(){
		String id = "1";
		Map result = new HashMap();
		result.put("status", State.WAITING_RUN);
		when(mongoTemplate.findOne(Query.query(Criteria.where("jobId").is(id)), Map.class, "jobtest")).thenReturn(result);
		stateMachine.handleEvent("1", Event.RUNNING);
		Mockito.verify(mongoTemplate, Mockito.times(1)).findOne(Query.query(Criteria.where("jobId").is(id)), Map.class, "jobtest");
	}
}
