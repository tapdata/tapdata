/**
 * @title: PipeHandler
 * @description:
 * @author lk
 * @date 2021/9/9
 */
package com.tapdata.tm.ws.handler;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.*;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.dto.WebSocketInfo;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.*;

@WebSocketMessageHandler(type = MessageType.PIPE)
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class PipeHandler implements WebSocketHandler {

    private final MessageQueueService queueService;
    private TaskDagCheckLogService taskDagCheckLogService;
    private final String MESSAGE_INFO_TYPE_OF_CONNECT_TEST = "testConnectionResult";//表示连接测试，在连接测试进行多语言处理时使用

    private DataSourceDefinitionService dataSourceDefinitionService;

    private UserService userService;

    public PipeHandler(MessageQueueService queueService) {
        this.queueService = queueService;
    }

    @Override
    public void handleMessage(WebSocketContext context) {
        MessageInfo messageInfo = context.getMessageInfo();

        Map<String, Object> data = messageInfo.getData();
        try {
            Object result = data.get("result");
            String jsonStr = JSON.toJSONString(result);
            JSONValidator jsonValidator = JSONValidator.from(jsonStr);
            jsonValidator.validate();
            if (jsonValidator.getType() == JSONValidator.Type.Object) {
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                if (Objects.nonNull(jsonObject)) {
                    JSONObject extParam = jsonObject.getJSONObject("extParam");
                    if (Objects.nonNull(extParam) && "testConnectionResult".equals(data.get("type").toString())) {
                        String taskId = extParam.getString("taskId");
                        String templateEnum = extParam.getString("templateEnum");
                        String userId = extParam.getString("userId");

                        if (org.apache.commons.lang3.StringUtils.isNotBlank(templateEnum)) {
                            JSONObject responseBody = jsonObject.getJSONObject("response_body");
                            JSONArray validateDetails = responseBody.getJSONArray("validate_details");

                            String grade = ("passed").equals(validateDetails.getJSONObject(0).getString("status")) ?
                                    Level.INFO.getValue() : Level.ERROR.getValue();

                            taskDagCheckLogService.createLog(taskId, userId, grade, DagOutputTemplateEnum.valueOf(templateEnum),
                                    true, true, DateUtil.now(), jsonObject.getJSONObject("response_body").toJSONString());
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("PipeHandler handleMessage response body error", e);
        }

        if (StringUtils.isNotBlank(messageInfo.getReceiver())) {
            if (messageInfo.getReceiver().equals(messageInfo.getSender())) {
                log.warn("The message ignore,the sender is the same as the receiver");
            } else {

                if(null != messageInfo.getData()) {
                    //对链接测试进行多语言处理
                    final String messageInfoType = String.valueOf(messageInfo.getData().get("type"));
                    if (MESSAGE_INFO_TYPE_OF_CONNECT_TEST.equals(messageInfoType)) {
                        this.multilingualTestConnection(context);
                    }
                }
                MessageQueueDto messageDto = new MessageQueueDto();
                BeanUtils.copyProperties(messageInfo, messageDto);
                queueService.sendMessage(messageDto);
            }
        } else {
            log.warn("WebSocket send message failed, receiver is blank, context: {}", JsonUtil.toJson(context));
        }
    }

    public void multilingualTestConnection(WebSocketContext context){
        UserDetail userDetail = userService.loadUserById(new ObjectId(context.getUserId()));
        if (null == userDetail) return;
        this.multilingualTestConnection(
                context.getMessageInfo().getReceiver(),
                "a5af410b12afca476edf4a650c133ddf135bf76542a67787ed6f7f7d53ba712",
                userDetail,
                context.getMessageInfo(),
                null);
    }

    //链接测试多语言处理
    private void multilingualTestConnection(
            String sessionId,
            String pdkHash,
            UserDetail user,
            MessageInfo messageInfo,
            String... field) {
        final DataSourceDefinitionDto dataSourceDefinition = dataSourceDefinitionService.findByPdkHash(pdkHash, user, field);
        if (!Objects.isNull(dataSourceDefinition)
                && !Objects.isNull(dataSourceDefinition.getProperties())) {
            Map<String,Object> testConnectionData = messageInfo.getData();
            if (testConnectionData == null) {
                return;
            }

            final String[] testConnectionStringArr = {JSON.toJSONString(testConnectionData)};

            LinkedHashMap<String, Object> messageConfigure = dataSourceDefinition.getMessages();
            String language = getLang(sessionId);

            if (messageConfigure == null) {
                return;
            }
            Object obj = messageConfigure.get(language);
            if(null == obj){
                return;
            }

            LinkedHashMap<String, Object> msgJson = JSON.parseObject(
                    JSON.toJSONString(obj),
                    new TypeReference<LinkedHashMap<String, Object>>(){}
                    );
            if(null == msgJson){
                return;
            }

            msgJson.forEach(
                    (key, value) ->
                            testConnectionStringArr[0] = testConnectionStringArr[0].replaceAll(
                                    new StringJoiner(key).add("\\$\\{").add("}").toString(), value.toString()
                            ));
            LinkedHashMap<String, Object> temp = JSON.parseObject(
                    testConnectionStringArr[0],
                    new TypeReference<LinkedHashMap<String, Object>>(){}
                    );

            messageInfo.setData(temp);
        }
    }

    private String getLang(String sessionId){
        Map<String, WebSocketInfo> connectCount = WebSocketManager.getConnectCount();

        String language = connectCount.get(sessionId).getLanguage();
        if (null==language || "".equals(language)){
            language = "en_US";
        }
        language = org.apache.commons.lang3.StringUtils.replace(language, "-", "_");
        return language;
    }
}
