/**
 * @title: StateMachineConfigurer
 * @description:
 * @author lk
 * @date 2021/7/30
 */
package com.tapdata.job.state;

public class StateMachineConfigurer {

	public StateMachineBuilder build(){
		return build(null);
	}

	public StateMachineBuilder build(StateMachineBuilder builder){
		if (builder == null){
			builder = new StateMachineBuilder();
		}
		configure(builder);

		return builder;
	}

	public void configure(StateMachineBuilder builder) {
		builder.transition()
//				.source(State.EDIT)
//				.target(State.EDIT)
//				.event(Event.E1)
//				.gurad((o)-> {
//					System.out.println("gurad1=======source: " + o.getSource().name() + ",event: " + o.getEvent().name() + ",target: " + o.getTarget().name());
//					return true;
//				})
//				.gurad((o)-> {
//					System.out.println("gurad2=======source: " + o.getSource().name() + ",event: " + o.getEvent().name() + ",target: " + o.getTarget().name());
//					return true;
//				})
//				.gurad((o)-> {
//					System.out.println("gurad3=======source: " + o.getSource().name() + ",event: " + o.getEvent().name() + ",target: " + o.getTarget().name());
//					return true;
//				})
//				.action((o)-> System.out.println("action1=======source: " + o.getSource().name() + ",event: " + o.getEvent().name() + ",target: " + o.getTarget().name()))
//				.action((o)-> System.out.println("action2=======source: " + o.getSource().name() + ",event: " + o.getEvent().name() + ",target: " + o.getTarget().name()))
//				.action((o)-> System.out.println("action3=======source: " + o.getSource().name() + ",event: " + o.getEvent().name() + ",target: " + o.getTarget().name()))
//				.and()
//				.transition()
				//1.人工：启动任务 编辑中-》调度中
				.source(State.EDIT)
				.target(State.SCHEDULING)
				.event(Event.START)
				.and()
				//2.人工：停止任务 调度中-》调度失败
				.transition()
				.source(State.SCHEDULING)
				.target(State.SCHEDULING_FAILED)
				.event(Event.STOPPING)
				.and()
				//2.事件+超时：持续无法调度 调度中-》调度失败
				.transition()
				.source(State.SCHEDULING)
				.target(State.SCHEDULING_FAILED)
				.event(Event.SCHEDULE_FAILED)
				.and()
				//3.事件：调度成功 调度中-》待运行
				.transition()
				.source(State.SCHEDULING)
				.target(State.WAITING_RUN)
				.event(Event.SCHEDULE_SUCCESS)
				.and()
				//4.人工：启动任务 调度失败-》调度中
				.transition()
				.source(State.SCHEDULING_FAILED)
				.target(State.SCHEDULING)
				.event(Event.START)
				.and()
				//5.人工：编辑任务 调度失败-》编辑中
				.transition()
				.source(State.SCHEDULING_FAILED)
				.target(State.EDIT)
				.event(Event.SCHEDULE_RESTART)
				.and()
				//6.超时：引擎接管超时（云版不使用） 待运行-》调度中
				.transition()
				.source(State.WAITING_RUN)
				.target(State.SCHEDULING)
				.event(Event.OVERTIME)
				.and()
				//6.人工：重新调度 待运行-》调度中
				.transition()
				.source(State.WAITING_RUN)
				.target(State.SCHEDULING)
				.event(Event.SCHEDULE_RESTART)
				.and()
				//7.事件：引擎接管运行 待运行-》运行中
				.transition()
				.source(State.WAITING_RUN)
				.target(State.RUNNING)
				.event(Event.RUNNING)
				.action((context)-> System.out.println("job is running,event: " + context.getEvent()))
				.and()
				//8.人工：停止任务 待运行-》停止中
				.transition()
				.source(State.WAITING_RUN)
				.target(State.STOPPING)
				.event(Event.STOPPING)
				.and()
				//9.超时：引擎心跳超时 运行中-》待运行
				.transition()
				.source(State.RUNNING)
				.target(State.WAITING_RUN)
				.event(Event.OVERTIME)
				.and()
				//9.事件：引擎正常退出 运行中-》待运行
				.transition()
				.source(State.RUNNING)
				.target(State.WAITING_RUN)
				.event(Event.EXIT)
				.and()
				//10.事件：任务运行完成 运行中-》任务完成
				.transition()
				.source(State.RUNNING)
				.target(State.DONE)
				.event(Event.COMPLETED)
				.and()
				//11.事件：任务执行错误 运行中-》错误
				.transition()
				.source(State.RUNNING)
				.target(State.ERROR)
				.event(Event.ERROR)
				.and()
				//12.人工：停止任务 运行中-》停止中
				.transition()
				.source(State.RUNNING)
				.target(State.STOPPING)
				.event(Event.STOPPING)
				.and()
				//13.人工：编辑任务 错误-》编辑中
				.transition()
				.source(State.ERROR)
				.target(State.EDIT)
				.event(Event.EDIT)
				.and()
				//14.人工：启动任务 错误-》调度中
				.transition()
				.source(State.ERROR)
				.target(State.SCHEDULING)
				.event(Event.START)
				.and()
				//15.事件：任务执行错误 停止中-》错误
				.transition()
				.source(State.STOPPING)
				.target(State.ERROR)
				.event(Event.ERROR)
				.and()
				//16.事件：任务执行完成 停止中-》已完成
				.transition()
				.source(State.STOPPING)
				.target(State.DONE)
				.event(Event.COMPLETED)
				.and()
				//17.事件：任务停止完成 停止中-》已停止
				.transition()
				.source(State.STOPPING)
				.target(State.STOPPED)
				.event(Event.STOPPED)
				.and()
				//17人工：强行停止任务 停止中-》已停止
				.transition()
				.source(State.STOPPING)
				.target(State.STOPPED)
				.event(Event.FORCE_STOP)
				.and()
				//18.人工：编辑任务 已停止-》编辑中
				.transition()
				.source(State.STOPPED)
				.target(State.EDIT)
				.event(Event.EDIT)
				.and()
				//19.人工：启动任务 已停止-》调度中
				.transition()
				.source(State.STOPPED)
				.target(State.SCHEDULING)
				.event(Event.START)
				.and()
				//20.人工：编辑任务 已完成-》编辑中
				.transition()
				.source(State.DONE)
				.target(State.EDIT)
				.event(Event.EDIT)
				.and()
				//21.人工：启动任务 已完成-》调度中
				.transition()
				.source(State.DONE)
				.target(State.SCHEDULING)
				.event(Event.START);
	}
}
