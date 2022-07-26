///**
// * @title: TestStateFail
// * @description:
// * @author lk
// * @date 2021/8/12
// */
//package com.tapdata.statemachine.test;
//
//import com.tapdata.tm.statemachine.enums.Event;
//import com.tapdata.tm.statemachine.enums.State;
//import org.junit.Assert;
//import org.junit.Test;
//
//public class TestStateAll extends BaseStateMachineTest {
//
//	@Test
//	public void test(){
//		int success = 0;
//		int fail = 0;
//		for (State state : State.values()) {
//			for (Event event : Event.values()) {
//				System.out.println("============start=================");
//				System.out.println("source: " + state.getName() + ", event: " + event.getName());
//				try {
//					handle(state, event, false);
//					success++;
//				}catch (Exception e){
//					System.out.println(e.getMessage());
//					fail++;
//				}
//				System.out.println("=============end==================\n\n");
//
//			}
//		}
//		System.out.println("成功:" + success + ",失败:" + fail);
//	}
//
//	@Test
//	public void testState() {
//		State status = State.getState("error");
//		Assert.assertNotNull(status);
//	}
//}
