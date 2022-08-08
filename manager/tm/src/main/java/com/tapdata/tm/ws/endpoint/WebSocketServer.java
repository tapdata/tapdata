/**
 * @title: WebSocketServer
 * @description:
 * @author lk
 * @date 2021/9/7
 */
package com.tapdata.tm.ws.endpoint;

import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.dto.WebSocketInfo;
import com.tapdata.tm.ws.enums.MessageType;
import com.tapdata.tm.ws.handler.WebSocketHandler;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@Slf4j
@Component
public class WebSocketServer extends TextWebSocketHandler {

	@Autowired
	private UserService userService;

	@Autowired
	private AccessTokenService accessTokenService;

	/**
	 * 握手后建立成功
	 */
	@Override
	public void afterConnectionEstablished(WebSocketSession session) {
		String id = getId(session);
		String userId = getUserId(session);
		log.info("WebSocket connect,id: {},userId: {}", id, userId);
		WebSocketManager.addSession(id, userId, session);
	}

	/**
	 * 接收消息
	 */
	@Override
	protected void handleTextMessage(WebSocketSession session, TextMessage message) {
		String id = getId(session);
		String msg = message.getPayload();
		WebSocketInfo webSocketInfo = WebSocketManager.getSessionById(session.getId());
		String userId = null;
		if (webSocketInfo != null){
			userId = webSocketInfo.getUserId();
		}
		log.info("WebSocket receive.  message, userId,id: {}, {}, {}", msg, userId, id);

 		if(StringUtils.isNotBlank(msg)){
			try {
				MessageInfo messageInfo = JsonUtil.parseJsonUseJackson(msg, MessageInfo.class);

				if (MessageType.PIPE.getType().equals(messageInfo.getType())) {
					Map<String, Object> data = messageInfo.getData();
					if (data != null) {
						Object type = data.get("type");
						if (type instanceof String) {
							if (((String)type).startsWith(MessageType.DATA_SYNC.getType())) {
								List<WebSocketHandler> handlers = WebSocketManager.getHandler(MessageType.DATA_SYNC.getType());
								WebSocketContext webSocketContext = new WebSocketContext(session.getId(), id, userId, messageInfo);
								for (WebSocketHandler handler : handlers) {
									handler.handleMessage(webSocketContext);
								}
								return;
							}
						}
					}
				}

				List<WebSocketHandler> handlers = WebSocketManager.getHandler(messageInfo.getType());
				if (CollectionUtils.isNotEmpty(handlers)){
					WebSocketContext webSocketContext = new WebSocketContext(session.getId(), id, userId, messageInfo);
					for (WebSocketHandler handler : handlers) {
						handler.handleMessage(webSocketContext);
					}
				}else {
//					WebSocketManager.sendMessageBySessionId(session.getId(), "Handle message error,handler is empty");
//					session.sendMessage(new TextMessage("Handle message error,handler is empty"));
					log.warn("Handle message error,handler is empty");
				}
			}catch (Exception e){
				log.error("Handle message error,sessionMsg: {} message: {}",JsonUtil.toJson(message), e.getMessage(), e);
//				try {
//					WebSocketManager.sendMessageBySessionId(session.getId(), String.format("Handle message error,message: %s", e.getMessage()));
//					session.sendMessage(new TextMessage(String.format("Handle message error,message: %s", e.getMessage())));
//				} catch (IOException ex) {
//					log.error("WebSocket send message failed,message: {}", e.getMessage(), e);
//				}
			}
		}
	}

	/**
	 * 断开连接时
	 */
	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
		String id = session.getId();
		WebSocketInfo webSocketInfo = WebSocketManager.getSessionById(id);
		log.error("WebSocket close,id: {},userId: {},closeStatus: {}", id, webSocketInfo != null ? webSocketInfo.getUserId() : null, JsonUtil.toJson(status));
		WebSocketManager.removeSession(id);
	}

	private String getId(WebSocketSession session){
		String id = getAgentId(session);
		if (StringUtils.isBlank(id)){
			id = session.getId();
		}
		return id;
	}

	private String getAgentId(WebSocketSession session){
		try {
			if (session.getUri() != null){
				Map<String, String> queryStrMap = queryStr2Map(session.getUri().getQuery());
				return queryStrMap.get("agentId");
			}
		}catch (Exception e){
			log.error("WebSocket get agentId error,message: {}", e.getMessage(), e);
		}
		return null;
	}

	private String getUserId(WebSocketSession session){
		try {
			List<String> userIds = session.getHandshakeHeaders().get("user_id");
			if (CollectionUtils.isNotEmpty(userIds)){
				UserDetail userDetail = userService.loadUserByExternalId(userIds.get(0));
				return userDetail != null ? userDetail.getUserId() : null;
			}else if (session.getUri() != null){
				Map<String, String> queryStrMap = queryStr2Map(session.getUri().getQuery());
				String accessToken = queryStrMap.get("access_token");
				ObjectId userId = accessTokenService.validate(accessToken);
				return userId != null ? userId.toHexString() : null;
			}
		}catch (Exception e){
			log.error("WebSocket get userId error,message: {}", e.getMessage(), e);
		}

		return null;
	}

	private Map<String, String> queryStr2Map(String queryStr){
		Map<String, String> result = new HashMap<>();
		if (StringUtils.isBlank(queryStr)){
			return result;
		}

		result = Arrays.stream(queryStr.split("&"))
				.map(kv -> kv.split("="))
				.collect(Collectors.toMap(split -> split[0], split -> split[1], (a, b) -> b));

		return result;
	}

}
