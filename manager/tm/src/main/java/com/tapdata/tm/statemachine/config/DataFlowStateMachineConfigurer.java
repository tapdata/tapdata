/**
 * @title: StateMachineConfigurer
 * @description:
 * @author lk
 * @date 2021/7/30
 */
package com.tapdata.tm.statemachine.config;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.dataflow.service.DataFlowService;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.statemachine.annotation.EnableStateMachine;
import com.tapdata.tm.statemachine.configuration.AbstractStateMachineConfigurer;
import com.tapdata.tm.statemachine.configuration.StateMachineBuilder;
import com.tapdata.tm.statemachine.constant.StateMachineConstant;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.enums.DataFlowState;
import com.tapdata.tm.statemachine.model.DataFlowStateContext;
import com.tapdata.tm.statemachine.model.StateContext;
import com.tapdata.tm.statemachine.model.StateMachineResult;

import java.util.Date;
import java.util.function.Function;

import com.tapdata.tm.user.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

@EnableStateMachine
@Slf4j
public class DataFlowStateMachineConfigurer extends AbstractStateMachineConfigurer<DataFlowState, DataFlowEvent> {

    @Autowired
    MessageService messageService;

    @Autowired
    UserService userService;

    public void configure(StateMachineBuilder<DataFlowState, DataFlowEvent> builder) {
        builder.transition()
                //1.人工：启动任务 编辑中-》调度中
                .source(DataFlowState.EDIT)
                .target(DataFlowState.SCHEDULING)
                .event(DataFlowEvent.START)
                .and()
                //2.人工：停止任务 调度中-》调度失败
                .transition()
                .source(DataFlowState.SCHEDULING)
                .target(DataFlowState.SCHEDULING_FAILED)
                .event(DataFlowEvent.STOP)
                .and()
                //2.事件+超时：持续无法调度 调度中-》调度失败
                .transition()
                .source(DataFlowState.SCHEDULING)
                .target(DataFlowState.SCHEDULING_FAILED)
                .event(DataFlowEvent.SCHEDULE_FAILED)
                .and()
                //3.事件：调度成功 调度中-》待运行
                .transition()
                .source(DataFlowState.SCHEDULING)
                .target(DataFlowState.WAITING_RUN)
                .event(DataFlowEvent.SCHEDULE_SUCCESS)
                .and()
                //4.人工：启动任务 调度失败-》调度中
                .transition()
                .source(DataFlowState.SCHEDULING_FAILED)
                .target(DataFlowState.SCHEDULING)
                .event(DataFlowEvent.START)
                .and()
                //5.人工：编辑任务 调度失败-》编辑中
                .transition()
                .source(DataFlowState.SCHEDULING_FAILED)
                .target(DataFlowState.EDIT)
                .event(DataFlowEvent.EDIT)
                .and()
                //6.超时：引擎接管超时（云版不使用） 待运行-》调度中
                .transition()
                .source(DataFlowState.WAITING_RUN)
                .target(DataFlowState.SCHEDULING)
                .event(DataFlowEvent.OVERTIME)
                .and()
                //6.人工：重新调度 待运行-》调度中
                .transition()
                .source(DataFlowState.WAITING_RUN)
                .target(DataFlowState.SCHEDULING)
                .event(DataFlowEvent.SCHEDULE_RESTART)
                .and()
                //7.事件：引擎接管运行 待运行-》运行中
                .transition()
                .source(DataFlowState.WAITING_RUN)
                .target(DataFlowState.RUNNING)
                .event(DataFlowEvent.RUNNING)
                .and()
                //8.人工：停止任务 待运行-》停止中
                .transition()
                .source(DataFlowState.WAITING_RUN)
                .target(DataFlowState.STOPPING)
                .event(DataFlowEvent.STOP)
                .and()
                //9.超时：引擎心跳超时 运行中-》待运行
                .transition()
                .source(DataFlowState.RUNNING)
                .target(DataFlowState.SCHEDULING) //  使用原有逻辑，运行中-》调度中
                .event(DataFlowEvent.OVERTIME)
                .and()
                //9.事件：引擎正常退出 运行中-》待运行
                .transition()
                .source(DataFlowState.RUNNING)
                .target(DataFlowState.WAITING_RUN)
                .event(DataFlowEvent.EXIT)
                .and()
                //10.事件：任务运行完成 运行中-》任务完成
                .transition()
                .source(DataFlowState.RUNNING)
                .target(DataFlowState.DONE)
                .event(DataFlowEvent.COMPLETED)
                .and()
                //11.事件：任务执行错误 运行中-》错误
                .transition()
                .source(DataFlowState.RUNNING)
                .target(DataFlowState.ERROR)
                .event(DataFlowEvent.ERROR)
                .and()
                //12.人工：停止任务 运行中-》停止中
                .transition()
                .source(DataFlowState.RUNNING)
                .target(DataFlowState.STOPPING)
                .event(DataFlowEvent.STOP)
                .and()
                //13.人工：编辑任务 错误-》编辑中
                .transition()
                .source(DataFlowState.ERROR)
                .target(DataFlowState.EDIT)
                .event(DataFlowEvent.EDIT)
                .and()
                //14.人工：启动任务 错误-》调度中
                .transition()
                .source(DataFlowState.ERROR)
                .target(DataFlowState.SCHEDULING)
                .event(DataFlowEvent.START)
                .and()
                //15.事件：任务执行错误 停止中-》错误
                .transition()
                .source(DataFlowState.STOPPING)
                .target(DataFlowState.ERROR)
                .event(DataFlowEvent.ERROR)
                .and()
                //16.事件：任务执行完成 停止中-》已完成
                .transition()
                .source(DataFlowState.STOPPING)
                .target(DataFlowState.DONE)
                .event(DataFlowEvent.COMPLETED)
                .and()
                //17.事件：任务停止完成 停止中-》已停止
                .transition()
                .source(DataFlowState.STOPPING)
                .target(DataFlowState.STOPPED)
                .event(DataFlowEvent.STOPPED)
                .and()
                //17人工：强行停止任务 停止中-》已停止
                .transition()
                .source(DataFlowState.STOPPING)
                .target(DataFlowState.STOPPED)
                .event(DataFlowEvent.FORCE_STOP)
                .and()
                //17事件：停止任务超时 停止中-》已停止
                .transition()
                .source(DataFlowState.STOPPING)
                .target(DataFlowState.STOPPED)
                .event(DataFlowEvent.OVERTIME)
                .and()
                //17事件：停止任务超时 强制停止中-》已停止
                .transition()
                .source(DataFlowState.FORCE_STOPPING)
                .target(DataFlowState.STOPPED)
                .event(DataFlowEvent.OVERTIME)
                .and()
                //18.人工：编辑任务 已停止-》编辑中
                .transition()
                .source(DataFlowState.STOPPED)
                .target(DataFlowState.EDIT)
                .event(DataFlowEvent.EDIT)
                .and()
                //19.人工：启动任务 已停止-》调度中
                .transition()
                .source(DataFlowState.STOPPED)
                .target(DataFlowState.SCHEDULING)
                .event(DataFlowEvent.START)
                .and()
                //20.人工：编辑任务 已完成-》编辑中
                .transition()
                .source(DataFlowState.DONE)
                .target(DataFlowState.EDIT)
                .event(DataFlowEvent.EDIT)
                .and()
                //21.人工：启动任务 已完成-》调度中
                .transition()
                .source(DataFlowState.DONE)
                .target(DataFlowState.SCHEDULING)
                .event(DataFlowEvent.START);

    }

    @Override
    public Class<? extends StateContext<DataFlowState, DataFlowEvent>> getContextClass() {
        return DataFlowStateContext.class;
    }

    @Autowired
    private DataFlowService dataFlowService;

    public Function<StateContext<DataFlowState, DataFlowEvent>, StateMachineResult> commonAction() {
        return (stateContext) -> {
            if (stateContext instanceof DataFlowStateContext) {
                Update update = Update.update("status", stateContext.getTarget().getName());
                setOperTime(stateContext.getTarget().getName(), update);
                UpdateResult updateResult = dataFlowService.update(
                        Query.query(Criteria.where("_id").is(((DataFlowStateContext) stateContext).getData().getId())
                                .and("status").is(stateContext.getSource().getName())),
                        update, stateContext.getUserDetail());

                //增加message
              /*  String targetStatus = stateContext.getTarget().getName();
                addMsg(targetStatus, (DataFlowStateContext) stateContext);*/

                if (updateResult.wasAcknowledged() && updateResult.getModifiedCount() > 0) {
                    stateContext.setNeedPostProcessor(true);
                }
                return StateMachineResult.ok(updateResult.getModifiedCount());
            }
            return StateMachineResult.fail("stateContext is not instance of DataFlowStateContext");
        };
    }

    private void setOperTime(String status, Update update) {
        log.info("setOperTime status:{}",status);
        Date date = new Date();
        switch (status) {
            case StateMachineConstant.DATAFLOW_STATUS_SCHEDULING:  //  scheduled对应startTime和scheduledTime
                update.set("startTime", date).set("scheduledTime", date);
                break;
            case StateMachineConstant.DATAFLOW_STATUS_STOPPING:  // stopping对应stoppingTime
                update.set("stoppingTime", date);
                break;
            case "force stopping":  //  force stopping对应forceStoppingTime
                update.set("forceStoppingTime", date);
                break;
            case StateMachineConstant.DATAFLOW_STATUS_RUNNING:  //  running对应runningTime
                update.set("runningTime", date);
                break;
            case StateMachineConstant.DATAFLOW_STATUS_ERROR:  //  error对应errorTime和finishTime
                update.set("errorTime", date).set("finishTime", date);
                log.info("任务变为已停止");
                break;
            case StateMachineConstant.DATAFLOW_STATUS_STOPPED:  //   paused对应pausedTime和finishTime
                update.set("pausedTime", date).set("finishTime", date);
                break;
            default:
                break;
        }
    }


    //已废弃  迁移任务都通过DataFlowController 来加
  /*  private void addMsg(String targetStatus, DataFlowStateContext dataFlowStateContext) {
        log.info("addMsg dataFlowStateContext:{}", JsonUtil.toJson(dataFlowStateContext));
        DataFlowDto dataFlowDto = dataFlowStateContext.getData();
        MsgTypeEnum msgTypeEnum = null;
        Level level=Level.INFO;
        if (StateMachineConstant.DATAFLOW_STATUS_ERROR.equals(targetStatus)) {
            log.info("任务出错  dataFlowStateContext:{}", JsonUtil.toJson(dataFlowStateContext));
            msgTypeEnum = MsgTypeEnum.CONNECTION_INTERRUPTED;
            level=Level.ERROR;
        } else if (StateMachineConstant.DATAFLOW_STATUS_STOPPED.equals(targetStatus)) {
            log.info("任务暂停  dataFlowStateContext:{}", JsonUtil.toJson(dataFlowStateContext));
            msgTypeEnum = MsgTypeEnum.PAUSED;
            level=Level.INFO;

        } else if (StateMachineConstant.DATAFLOW_EVENT_START.equals(targetStatus)) {
            log.info("任务已启动  dataFlowStateContext:{}", JsonUtil.toJson(dataFlowStateContext));
            msgTypeEnum = MsgTypeEnum.STARTED;
            level=Level.INFO;
        }
        UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(dataFlowDto.getUserId()));
        messageService.add(dataFlowDto.getName(), dataFlowDto.getId().toString(), msgTypeEnum, SystemEnum.DATAFLOW, dataFlowDto.getId().toHexString(),level, userDetail);
    }*/
}
