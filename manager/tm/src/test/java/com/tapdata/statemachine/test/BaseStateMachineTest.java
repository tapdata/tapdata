///**
// * @title: BaseStateMachineTest
// * @description:
// * @author lk
// * @date 2021/8/11
// */
//package com.tapdata.statemachine.test;
//
//import com.mongodb.client.result.UpdateResult;
//import com.tapdata.tm.statemachine.Processor;
//import com.tapdata.tm.statemachine.enums.Event;
//import com.tapdata.tm.statemachine.enums.State;
//import com.tapdata.tm.statemachine.model.DataFlow;
//import com.tapdata.tm.statemachine.model.StateContext;
//import com.tapdata.tm.statemachine.model.StateTrigger;
//import com.tapdata.tm.statemachine.model.Transition;
//import com.tapdata.tm.statemachine.processor.DefaultStateMachineProcessor;
//import com.tapdata.tm.statemachine.processor.StartStateMachineProcessor;
//import com.tapdata.tm.statemachine.service.StateMachine;
//import com.tapdata.tm.statemachine.service.StateMachineExecutor;
//import com.tapdata.tm.statemachine.utils.StateMachineUtils;
//import java.util.Map;
//import org.apache.commons.collections4.MapUtils;
//import org.bson.BsonInt32;
//import org.junit.Before;
//import org.junit.runner.RunWith;
//import org.mockito.InjectMocks;
//import org.mockito.Mock;
//import org.mockito.Mockito;
//import org.mockito.Spy;
//import static org.powermock.api.mockito.PowerMockito.when;
//import org.powermock.core.classloader.annotations.PowerMockIgnore;
//import org.powermock.modules.junit4.PowerMockRunner;
//import org.springframework.data.mongodb.core.MongoTemplate;
//import org.springframework.data.mongodb.core.query.Criteria;
//import org.springframework.data.mongodb.core.query.Query;
//import org.springframework.data.mongodb.core.query.Update;
//
//@RunWith(PowerMockRunner.class)
//@PowerMockIgnore(value = "javax.management.*")
//public class BaseStateMachineTest {
//
//	@Mock
//	MongoTemplate mongoTemplate;
//	@Spy
//	StateMachineExecutor executor = new StateMachineExecutor();
//	@InjectMocks
//	StateMachine stateMachine;
//
//	@InjectMocks
//	StartStateMachineProcessor startStateMachineProcessor;
//
//	@InjectMocks
//	DefaultStateMachineProcessor defaultStateMachineProcessor;
//
//	@Before
//	public void before(){
//		putProcessor(State.EDIT, Event.START, startStateMachineProcessor);
//		putProcessor(State.SCHEDULING, Event.STOP, defaultStateMachineProcessor);
//		putProcessor(State.SCHEDULING, Event.SCHEDULE_FAILED, defaultStateMachineProcessor);
//		putProcessor(State.SCHEDULING, Event.SCHEDULE_SUCCESS, defaultStateMachineProcessor);
//		putProcessor(State.SCHEDULING_FAILED, Event.START, defaultStateMachineProcessor);
//		putProcessor(State.SCHEDULING_FAILED, Event.EDIT, defaultStateMachineProcessor);
//		putProcessor(State.WAITING_RUN, Event.OVERTIME, defaultStateMachineProcessor);
//		putProcessor(State.WAITING_RUN, Event.SCHEDULE_RESTART, defaultStateMachineProcessor);
//		putProcessor(State.WAITING_RUN, Event.RUNNING, defaultStateMachineProcessor);
//		putProcessor(State.WAITING_RUN, Event.STOP, defaultStateMachineProcessor);
//		putProcessor(State.RUNNING, Event.OVERTIME, defaultStateMachineProcessor);
//		putProcessor(State.RUNNING, Event.EXIT, defaultStateMachineProcessor);
//		putProcessor(State.RUNNING, Event.COMPLETED, defaultStateMachineProcessor);
//		putProcessor(State.RUNNING, Event.ERROR, defaultStateMachineProcessor);
//		putProcessor(State.RUNNING, Event.STOP, defaultStateMachineProcessor);
//		putProcessor(State.ERROR, Event.EDIT, defaultStateMachineProcessor);
//		putProcessor(State.ERROR, Event.START, defaultStateMachineProcessor);
//		putProcessor(State.STOPPING, Event.ERROR, defaultStateMachineProcessor);
//		putProcessor(State.STOPPING, Event.COMPLETED, defaultStateMachineProcessor);
//		putProcessor(State.STOPPING, Event.STOPPED, defaultStateMachineProcessor);
//		putProcessor(State.STOPPING, Event.FORCE_STOP, defaultStateMachineProcessor);
//		putProcessor(State.STOPPED, Event.EDIT, defaultStateMachineProcessor);
//		putProcessor(State.STOPPED, Event.START, defaultStateMachineProcessor);
//		putProcessor(State.DONE, Event.EDIT, defaultStateMachineProcessor);
//		putProcessor(State.DONE, Event.START, defaultStateMachineProcessor);
//		executor.init();
//	}
//
//	private void putProcessor(State source, Event event, Processor processor){
//		String key = String.format("%s_%s", source.getName(), event.getName());
//		StateMachineUtils.stateMachineProcessorHashMap.put(key, processor);
//	}
//
//	void handle(State source, Event event, boolean verify){
//		StateTrigger trigger = new StateTrigger();
//		trigger.setDataFlowId("1");
//		trigger.setEvent(event);
//		handle(source, trigger, verify);
//	}
//
//	private State getTarget(State source,Event event){
//		State target = State.EDIT;
//		Map<Event, Transition> tranMap = StateMachineExecutor.getStateTransitionMap().get(source);
//		if (MapUtils.isNotEmpty(tranMap)){
//			Transition transition = tranMap.get(event);
//			if (transition != null){
//				target = transition.getTarget();
//			}
//		}
//
//		return target;
//	}
//
//
//	private void handle(State source, StateTrigger trigger, boolean verify){
//		StateContext context = new StateContext(source, trigger);
//		context.setTarget(getTarget(source, trigger.getEvent()));
//		mockData(context);
//		stateMachine.handleEvent(trigger);
//		if (verify){
//			mockVerify(context, 1);
//		}
//	}
//
//	private void mockData(StateContext context){
//		DataFlow dataFlow = new DataFlow();
//		dataFlow.setId(context.getDataFlowId());
//		dataFlow.setStatus(context.getSource().getName());
//		when(mongoTemplate.findOne(Query.query(Criteria.where("id").is(dataFlow.getId())), DataFlow.class, "DataFlow")).thenReturn(dataFlow);
//		when(mongoTemplate.updateFirst(Query.query(Criteria.where("id").is(dataFlow.getId()).and("status").is(dataFlow.getStatus())),
//				Update.update("status", context.getTarget().getName()), "DataFlow"))
//				.thenReturn(UpdateResult.acknowledged(1,1L,new BsonInt32(1)));
//	}
//
//	private void mockVerify(StateContext context, int times){
//		Mockito.verify(mongoTemplate, Mockito.times(times)).findOne(Query.query(Criteria.where("id").is(context.getDataFlowId())), DataFlow.class, "DataFlow");
//		Mockito.verify(mongoTemplate, Mockito.times(times)).updateFirst(Query.query(Criteria.where("id").is(context.getDataFlowId()).and("status").is(context.getSource().getName())),
//				Update.update("status", context.getTarget().getName()), "DataFlow");
//	}
//
//}
