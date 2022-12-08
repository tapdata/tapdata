package com.tapdata.tm.ws.handler;

import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.task.dto.DataSyncMq;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: Zed
 * @Date: 2021/11/11
 * @Description: 数据同步的handler
 */
@WebSocketMessageHandler(type = MessageType.DATA_SYNC)
@Slf4j
public class DataSyncHandler implements WebSocketHandler{

    public static final Map<String, Map<String, String>> dataSyncCache = new ConcurrentHashMap<>();

    private final TaskService taskService;
    private final UserService userService;
    public DataSyncHandler(TaskService taskService, UserService userService) {
        this.taskService = taskService;
        this.userService = userService;
    }

    @Override
    public void handleMessage(WebSocketContext context) {
        MessageInfo messageInfo = context.getMessageInfo();

        if (messageInfo == null) {
            try {
                WebSocketManager.sendMessage(context.getSender(), "Message data cannot be null");
            } catch (Exception e) {
                log.error("WebSocket send message failed, message: {}", e.getMessage());
            }
            return;
        }

        log.info("DataSyncHandler  messageInfo:{}", JsonUtil.toJson(messageInfo));
        String userId = context.getUserId();
        UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(userId));

        Map<String, Object> data = messageInfo.getData();

        DataSyncMq dataSyncMq = null;
        if (data.get("status").equals("SUCCESS")) {
            Object result = data.get("result");
            if (result != null) {
                String jsonMq = JsonUtil.toJsonUseJackson(result);
                dataSyncMq = JsonUtil.parseJsonUseJackson(jsonMq, DataSyncMq.class);
            }
        }

        if (dataSyncMq == null) {
            String json = JsonUtil.toJson(data);
            dataSyncMq = JsonUtil.parseJsonUseJackson(json, DataSyncMq.class);
        }

        if (dataSyncMq == null) {
            return;
        }

        ObjectId objectId = MongoUtils.toObjectId(dataSyncMq.getTaskId());
        switch (dataSyncMq.getOpType()) {
            case DataSyncMq.OP_TYPE_STARTED:
            case DataSyncMq.OP_TYPE_RESTARTED:
                //任务状态在运行中，可能收到运行已完成。
                //收到任务已经运行的消息，将子任务改成已运行状态
                log.info("subTask running status report by ws, id = {}", objectId);
                taskService.running(objectId, userDetail);
                break;
            case DataSyncMq.OP_TYPE_STOPPED:
                //收到任务已停止消息，如果子任务状态为暂停中，则将子任务改成已暂停，如果子任务状态为停止中，则改为已停止
                taskService.stopped(objectId, userDetail);
                break;
            case DataSyncMq.OP_TYPE_ERROR:
                //任务状态在运行中，可能收到运行错误。
                taskService.runError(objectId, userDetail, dataSyncMq.getErrMsg(), dataSyncMq.getErrStack());
                break;
            case DataSyncMq.OP_TYPE_COMPLETE:
                //任务状态在运行中，可能收到运行已完成。
                taskService.complete(objectId, userDetail);
                break;
//                case DataSyncMq.OP_TYPE_RESETED:
//                    //任务状态在运行中，可能收到运行已完成。
//                    taskService.reseted(objectId, userDetail);
//                    break;
//                case DataSyncMq.OP_TYPE_DELETED:
//                    //任务状态在运行中，可能收到运行已完成。
//                    taskService.deleted(objectId, userDetail);
//                    break;
//                case DataSyncMq.OP_TYPE_RESET_DELETE_REPORT:
//                    //任务状态在运行中，可能收到运行已完成。
//                    taskService.resetReport(objectId, userDetail, dataSyncMq.getResetEventDto());
//                    break;
            default:
                break;
        }
    }

    private void handleResult(String opType, ObjectId id, UserDetail user) {
        switch (opType) {

        }
    }
}
