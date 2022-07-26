///**
// * @title: TestState
// * @description:
// * @author lk
// * @date 2021/8/11
// */
//package com.tapdata.statemachine.test;
//
//import com.tapdata.tm.statemachine.enums.Event;
//import com.tapdata.tm.statemachine.enums.State;
//import org.junit.Test;
//
////@RunWith(PowerMockRunner.class)
////@PowerMockIgnore(value = "javax.management.*")
//public class TestStateSuccess extends BaseStateMachineTest{
//
//	/**
//	 * 启动任务  编辑中--》调度中
//	 **/
//	@Test
//	public void start(){
//		handleEvent(State.EDIT, Event.START);
//	}
//
//	/**
//	 * 停止任务  调度中--》调度失败
//	 **/
//	@Test
//	public void stop(){
//		handleEvent(State.SCHEDULING, Event.STOP);
//	}
//
//	/**
//	 * 无法调度（事件+超时）  调度中--》调度失败
//	 **/
//	@Test
//	public void scheduleFail(){
//		handleEvent(State.SCHEDULING, Event.SCHEDULE_FAILED);
//	}
//
//	/**
//	 * 调度成功  调度中--》待运行
//	 **/
//	@Test
//	public void scheduleSuccess(){
//		handleEvent(State.SCHEDULING, Event.SCHEDULE_SUCCESS);
//	}
//
//	/**
//	 * 启动任务  调度失败--》调度中
//	 **/
//	@Test
//	public void startDataFlow(){
//		handleEvent(State.SCHEDULING_FAILED, Event.START);
//	}
//
//	/**
//	 * 编辑任务  调度失败--》编辑中
//	 **/
//	@Test
//	public void editDataFlow(){
//		handleEvent(State.SCHEDULING_FAILED, Event.EDIT);
//	}
//
//	/**
//	 * 引擎接管超时  待运行--》调度中
//	 **/
//	@Test
//	public void engineOvertime(){
//		handleEvent(State.WAITING_RUN, Event.OVERTIME);
//	}
//
//	/**
//	 * 重新调度  待运行--》调度中
//	 **/
//	@Test
//	public void scheduleRestart(){
//		handleEvent(State.WAITING_RUN, Event.SCHEDULE_RESTART);
//	}
//
//	/**
//	 * 引擎接管运行  待运行--》运行中
//	 **/
//	@Test
//	public void engineRunning(){
//		handleEvent(State.WAITING_RUN, Event.RUNNING);
//	}
//
//	/**
//	 * 停止任务  待运行--》停止中
//	 **/
//	@Test
//	public void stopDataFlow(){
//		handleEvent(State.WAITING_RUN, Event.STOP);
//	}
//
//	/**
//	 * 引擎心跳超时  运行中--》待运行
//	 **/
//	@Test
//	public void engineOvertime2(){
//		handleEvent(State.RUNNING, Event.OVERTIME);
//	}
//
//	/**
//	 * 引擎正常退出  运行中--》待运行
//	 **/
//	@Test
//	public void engineExit(){
//		handleEvent(State.RUNNING, Event.EXIT);
//	}
//
//	/**
//	 * 任务运行完成  运行中--》任务完成
//	 **/
//	@Test
//	public void dataFlowCompleted(){
//		handleEvent(State.RUNNING, Event.COMPLETED);
//	}
//
//	/**
//	 * 任务执行错误  运行中--》错误
//	 **/
//	@Test
//	public void dataFlowError(){
//		handleEvent(State.RUNNING, Event.ERROR);
//	}
//
//	/**
//	 * 停止任务  运行中--》停止中
//	 **/
//	@Test
//	public void dataFlowStop(){
//		handleEvent(State.RUNNING, Event.STOP);
//	}
//
//	/**
//	 * 编辑任务  错误--》编辑中
//	 **/
//	@Test
//	public void editDataFlow2(){
//		handleEvent(State.ERROR, Event.EDIT);
//	}
//
//	/**
//	 * 启动任务  错误--》调度中
//	 **/
//	@Test
//	public void startDataFlow2(){
//		handleEvent(State.ERROR, Event.START);
//	}
//
//	/**
//	 * 任务执行错误  停止中--》错误
//	 **/
//	@Test
//	public void dataFlowError2(){
//		handleEvent(State.STOPPING, Event.ERROR);
//	}
//
//	/**
//	 * 任务执行完成  停止中--》已完成
//	 **/
//	@Test
//	public void dataFlowCompleted2(){
//		handleEvent(State.STOPPING, Event.COMPLETED);
//	}
//
//	/**
//	 * 任务停止完成  停止中--》已停止
//	 **/
//	@Test
//	public void dataFlowStopped(){
//		handleEvent(State.STOPPING, Event.STOPPED);
//	}
//
//	/**
//	 * 强行停止任务  停止中--》已停止
//	 **/
//	@Test
//	public void dataFlowForceStop(){
//		handleEvent(State.STOPPING, Event.FORCE_STOP);
//	}
//
//	/**
//	 * 编辑任务  已停止--》编辑中
//	 **/
//	@Test
//	public void editDataFlow3(){
//		handleEvent(State.STOPPED, Event.EDIT);
//	}
//
//	/**
//	 * 启动任务  已停止--》调度中
//	 **/
//	@Test
//	public void startDataFlow3(){
//		handleEvent(State.STOPPED, Event.START);
//	}
//
//	/**
//	 * 编辑任务  已完成--》编辑中
//	 **/
//	@Test
//	public void editDataFlow4(){
//		handleEvent(State.DONE, Event.EDIT);
//	}
//
//	/**
//	 * 启动任务  已完成--》调度中
//	 **/
//	@Test
//	public void startDataFlow4(){
//		handleEvent(State.DONE, Event.START);
//	}
//
//
//
//
//	private void handleEvent(State source, Event event){
//		try {
//			handle(source, event, true);
//		}catch (Exception e){
//			System.out.println("******************************************************************");
//			System.out.println(e.getMessage());
//			System.out.println("******************************************************************");
//		}
//	}
//
//}
