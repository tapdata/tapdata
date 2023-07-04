package com.tapdata.tm.ws.handler;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

@WebSocketMessageHandler(type = MessageType.DROP_TABLE)
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class DropTableHandler implements WebSocketHandler  {

    private UserService userService;

    @Override
    public void handleMessage(WebSocketContext context) throws Exception {
        MessageInfo messageInfo = context.getMessageInfo();
        messageInfo.getData().put("type", messageInfo.getType());
        messageInfo.setType("pipe");
        String userId = context.getUserId();
        if (StringUtils.isBlank(userId)){
            WebSocketManager.sendMessage(context.getSender(), "UserId is blank");
            return;
        }

        UserDetail userDetail = userService.loadUserById(toObjectId(userId));
        if (userDetail == null){
            WebSocketManager.sendMessage(context.getSender(), "UserDetail is null");
            return;
        }

        Map<String, Object> data = messageInfo.getData();
    }
}
