/**
 * @title: WebSocketServer
 * @description:
 * @author lk
 * @date 2021/9/7
 */
package com.tapdata.tm.ws.endpoint;

import cn.hutool.core.bean.BeanException;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.commons.websocket.AllowRemoteCall;
import com.tapdata.tm.commons.websocket.MessageInfoBuilder;
import com.tapdata.tm.commons.websocket.ReturnCallback;
import com.tapdata.tm.commons.websocket.v1.MessageInfoV1;
import com.tapdata.tm.commons.websocket.v1.ResultWrap;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.dto.WebSocketInfo;
import com.tapdata.tm.ws.enums.MessageType;
import com.tapdata.tm.ws.handler.WebSocketHandler;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

@Slf4j
@Component
public class WebSocketServer extends TextWebSocketHandler {
	private final static long MAX_INTERVAL_BEFORE_CLOSE_SESSION_AFTER_NO_PONG = 60 * 1000;

	@Autowired
	private UserService userService;

	@Autowired
	private AccessTokenService accessTokenService;

	@Autowired
	private ApplicationContext applicationContext;

	@Autowired
	private MessageQueueService messageQueueService;

	/**
	 * Send ping frame message every 10 seconds.
	 */
	@Scheduled(fixedDelay = 15000)
	public void checkConnectionKeepAlive() {

		log.debug("Send ping message to websocket clients");
		WebSocketManager.checkAlive(webSocketInfo -> {
			try {
				// close the session if the client does not respond the ping message with
				// pong over MAX_INTERVAL_BEFORE_CLOSE_SESSION_AFTER_NO_PONG
				if (System.currentTimeMillis() - webSocketInfo.getLastKeepAliveTimestamp() > MAX_INTERVAL_BEFORE_CLOSE_SESSION_AFTER_NO_PONG) {
					webSocketInfo.getSession().close();
					return false;
				}
				webSocketInfo.getSession().sendMessage(new PingMessage());
			} catch (IOException e) {
				log.error("Send ping message to client failed: userId {}, agentId {}, sessionId {}",
						webSocketInfo.getUserId(), webSocketInfo.getAgentId(), webSocketInfo.getId(), e);
				return false;
			}
			return true;
		});
	}

	/**
	 * 握手后建立成功
	 */
	@Override
	public void afterConnectionEstablished(WebSocketSession session) {
		String id = getId(session);
		String userId = getUserId(session);
		String agentId = getAgentId(session);
		String remoteIp = null;
		InetSocketAddress remote = session.getRemoteAddress();
		if (remote != null) {
			remoteIp = remote.getAddress().getHostAddress();
		}
		log.info("WebSocket connect,id: {},userId: {}, agentId: {}, remote address {}", id, userId, agentId, remoteIp);
		WebSocketInfo webSocketInfo = new WebSocketInfo(session.getId(), agentId, userId, session, remoteIp);
		WebSocketManager.addSession(webSocketInfo);
		try {
			session.sendMessage(new PingMessage());
		} catch (IOException e) {
			// ignore
		}
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
		log.info("WebSocket receive. message, userId {},id {}", userId, id);

		if (msg.startsWith(MessageInfoV1.VERSION)) {
			MessageInfoV1 messageInfo = MessageInfoV1.parse(msg);
			if (messageInfo != null) {
				String remoteIp = webSocketInfo != null ? webSocketInfo.getIp() : "";
				Thread.currentThread().setName(remoteIp + "-" + Thread.currentThread().getId() + "-" + messageInfo.getReqId());
				log.info("WebSocket receive. userId: {}, id: {}, message: {}", userId, id, msg);
				dispatchMessage(messageInfo, id, webSocketInfo);
			} else {
				log.error("Unsupported protocol for message: {}", msg);
			}
		} else if(StringUtils.isNotBlank(msg)){
			log.info("WebSocket receive. userId: {}, id: {}, message: {}", userId, id, msg);
			try {
				MessageInfo messageInfo = JsonUtil.parseJsonUseJackson(msg, MessageInfo.class);

				if (MessageType.PIPE.getType().equals(messageInfo.getType())) {
					Map<String, Object> data = messageInfo.getData();
					if (data != null) {
						Object type = data.get("type");
						if (type instanceof String) {
							if (((String)type).startsWith(MessageType.DATA_SYNC.getType())) {
								WebSocketHandler handler = WebSocketManager.getHandler(MessageType.DATA_SYNC.getType());
								if (handler != null) {
									WebSocketContext webSocketContext = new WebSocketContext(session.getId(), id, userId, messageInfo);
									handler.handleMessage(webSocketContext);
								} else {
									log.warn("Not found message handler for type {}, ignore handler message", messageInfo.getType());
								}
								return;
							}
						}
					}
				}

				WebSocketHandler handler = WebSocketManager.getHandler(messageInfo.getType());
				if (handler != null){
					WebSocketContext webSocketContext = new WebSocketContext(session.getId(), id, userId, messageInfo);
					handler.handleMessage(webSocketContext);
				} else {
//					WebSocketManager.sendMessageBySessionId(session.getId(), "Handle message error,handler is empty");
//					session.sendMessage(new TextMessage("Handle message error,handler is empty"));
					log.warn("Not found message handler for type {}, ignore handler message", messageInfo.getType());
				}
			}catch (Exception e){
				log.error("Handle message error,sessionMsg: {} message: {}, stack = {}", JsonUtil.toJson(message), e.getMessage(), e.getStackTrace());
//				try {
//					WebSocketManager.sendMessageBySessionId(session.getId(), String.format("Handle message error,message: %s", e.getMessage()));
//					session.sendMessage(new TextMessage(String.format("Handle message error,message: %s", e.getMessage())));
//				} catch (IOException ex) {
//					log.error("WebSocket send message failed,message: {}", e.getMessage(), e);
//				}
			}
		}
	}

	@Override
	protected void handlePongMessage(WebSocketSession session, PongMessage message) throws Exception {
		WebSocketInfo webSocketInfo = WebSocketManager.getSessionById(session.getId());
		if (webSocketInfo != null) {
			webSocketInfo.setLastKeepAliveTimestamp(System.currentTimeMillis());
		}
	}

	private void dispatchMessage(MessageInfoV1 messageInfo, String id, WebSocketInfo webSocketInfo) {

		if (messageInfo == null) {
			log.error("Invalid params, messageInfo can't be null.");
			return;
		}

		if (MessageInfoV1.RETURN_TYPE.equals(messageInfo.getType())) {
			if (WebSocketManager.containsResultCallback(messageInfo.getReqId())) {
				handlerResult(messageInfo);
			} else {
				MessageQueueDto messageQueueDto = new MessageQueueDto();
				messageQueueDto.setSender(id);
				messageQueueDto.setType("v1");
				messageQueueDto.setData(messageInfo.toTextMessage());
				messageQueueDto.setCreateAt(new Date());
				messageQueueService.save(messageQueueDto);
			}
			return;
		}

		if (messageInfo.getBeanName() == null || messageInfo.getMethodName() == null) {
			log.warn("Message type not contains beanName or methodName");
			return;
		}

		try {
			String methodName = messageInfo.getMethodName();
			Object bean = applicationContext.getBean(messageInfo.getBeanName());
			Method[] methods = AopUtils.getTargetClass(bean).getDeclaredMethods();
			List<Method> methodList = Stream.of(methods).filter(m -> m.getName().equals(methodName)).collect(Collectors.toList());
			if (methodList.size() == 0) {
				String errorMsg = "Can't not found bean method " +
						messageInfo.getBeanName() + "." + messageInfo.getMethodName() +
						" in " + bean.getClass().getSimpleName();
				log.error(errorMsg);
				replayMessage(messageInfo, webSocketInfo, "NotFoundMethod", errorMsg);
				return;
			}

			Optional<Method> methodOptional = methodList.stream().filter(m -> m.isAnnotationPresent(AllowRemoteCall.class)).findFirst();
			if (!methodOptional.isPresent()) {
				String errorMsg = "Method not allow remote call, please add @AllowRemoteCall annotation on your method to remote call.";
				log.error(errorMsg);
				replayMessage(messageInfo, webSocketInfo, "NotAllowRemoteCall", errorMsg);
				return;
			}

			Method method = methodOptional.get();
			method.setAccessible(true);
			Class<?>[] parameterTypes = method.getParameterTypes();
			Object[] arguments = new Object[parameterTypes.length];

			UserDetail userDetail = null;

			for (int i = 0; i < parameterTypes.length; i++) {
				if (parameterTypes[i] == String.class) {
					arguments[i] = messageInfo.getBody();
				} else if (parameterTypes[i] == UserDetail.class) {
					if (userDetail == null) {
						userDetail = userService.loadUserByExternalId(webSocketInfo.getUserId());
					}
					arguments[i] = userDetail;
				} else {
					arguments[i] = JsonUtil.parseJsonUseJackson(messageInfo.getBody(), parameterTypes[i]);
				}
			}

			boolean isVoid = "void".equals(method.getReturnType().getName());
			Object returnValue = method.invoke(bean, arguments);

			if (!isVoid) {
				if (returnValue instanceof String ||
						returnValue instanceof Character ||
						returnValue instanceof Number ||
						returnValue instanceof Boolean ||
						returnValue.getClass().isPrimitive()) {
					replayMessage(messageInfo, webSocketInfo, ResponseMessage.OK, null, returnValue);
				} else
					replayMessage(messageInfo, webSocketInfo, ResponseMessage.OK, null, JsonUtil.toJson(returnValue));
			} else {
				// ignore
			}
		} catch (NoSuchBeanDefinitionException e) {
			log.error("No bean named '{}' available", messageInfo.getBeanName(), e);
			replayMessage(messageInfo, webSocketInfo, "NotFoundBean",
					"Can't not found bean instance in application context whit name " + messageInfo.getBeanName());
		} catch (BeanException e) {
			log.error("Can't not found bean instance with name " + messageInfo.getBeanName(), e);
			replayMessage(messageInfo, webSocketInfo, "NotFoundBean",
					"Can't not found bean instance in application context whit name " + messageInfo.getBeanName());
		} catch (IllegalAccessException e) {
			String errorMessage = "Can't not call method " +
					messageInfo.getBeanName() + "." + messageInfo.getMethodName() + ", " + e.getMessage();
			log.error(errorMessage, e);
			replayMessage(messageInfo, webSocketInfo, "MethodIllegalAccess", errorMessage);
		} catch (InvocationTargetException e) {
			Throwable targetException = e.getTargetException() != null ? e.getTargetException() : e;
			String errorMessage = "Invoke method failed " +
					messageInfo.getBeanName() + "." + messageInfo.getMethodName() + ", " + targetException.getMessage();
			log.error(errorMessage, e);
			replayMessage(messageInfo, webSocketInfo, "MethodInvocationFailed", errorMessage);
		} catch (Throwable e) {
			String errorMessage = "Catch unknown exception " + e.getMessage();
			log.error(errorMessage, e);
			replayMessage(messageInfo, webSocketInfo, "UnknownException", errorMessage);
		}

	}

	public void handlerResult(MessageInfoV1 messageInfo) {
		if (messageInfo != null) {

			if (!MessageInfoV1.RETURN_TYPE.equals(messageInfo.getType())) {
				// ignore
				log.warn("Unsupported return type {}, {}", messageInfo.getType(), messageInfo);
				return ;
			}
			String body = messageInfo.getBody();
			ResultWrap result = JsonUtil.parseJsonUseJackson(body, ResultWrap.class);

			ReturnCallback<?> callback = WebSocketManager.getAndRemoveResultCallback(messageInfo.getReqId());
			if (callback != null) {
				Type[] type = ((ParameterizedTypeImpl) callback.getClass().getGenericSuperclass()).getActualTypeArguments();
				try {
					if (ResultWrap.OK.equals(result.getCode())) {
						Object argument = null;
						if (type[0] == String.class) {
							argument = result.getData();
						} else {
							argument = JsonUtil.parseJsonUseJackson(result.getData(), (Class<?>)type[0]);
						}

						//callback.success(argument);
						//callback.success(argument);
						Method method = callback.getClass().getMethod("success", (Class<?>) type[0]);
						method.setAccessible(true);
						method.invoke(callback, argument);
						log.info("Callback method success {}, argument {}", messageInfo.getReqId(), argument);
					} else {
						callback.error(result.getCode(), result.getMessage());
						log.info("Callback method success {}", messageInfo.getReqId());
					}

				} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
					log.error("Callback method failed {}", messageInfo, e);
				}
			} else {
				log.warn("Can't found callback for request {}, {}", messageInfo.getReqId(), messageInfo);
			}
		}
	}

	private void replayMessage(MessageInfoV1 messageInfo, WebSocketInfo webSocketInfo, String code, String message) {
		replayMessage(messageInfo, webSocketInfo, code, message, null);
	}
	private void replayMessage(MessageInfoV1 messageInfo, WebSocketInfo webSocketInfo, String code, String message, Object data) {
		ResultWrap resultWrap = new ResultWrap();
		resultWrap.setCode(code);
		if (data != null) {
			resultWrap.setData(data instanceof String ? (String)data : JsonUtil.toJsonUseJackson(data));
		}
		resultWrap.setMessage(message);

		com.tapdata.tm.commons.websocket.MessageInfo returnMessageInfo =
				MessageInfoBuilder.returnMessage(messageInfo)
						.body(JsonUtil.toJson(resultWrap)).build();
		try {
			webSocketInfo.getSession().sendMessage(new TextMessage(returnMessageInfo.toTextMessage()));
		} catch (Exception e1) {
			log.error("Failed to reply to message", e1);
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
			String id = getAttr(session, "agentId");
			if (StringUtils.isNotBlank(id)) {
				return id;
			}
			if (session.getUri() != null){
				Map<String, String> queryStrMap = queryStr2Map(session.getUri().getQuery());
				session.getAttributes().putAll(queryStrMap);
				return queryStrMap.get("agentId");
			}
		}catch (Exception e){
			log.error("WebSocket get agentId error,message: {}", e.getMessage());
		}
		return null;
	}

	private String getAttr(WebSocketSession session, String attrName) {
		if (session != null && session.getAttributes().containsKey(attrName)) {
			Object val = session.getAttributes().get("agentId");
			if ( val != null) {
				return val.toString();
			}
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
			log.error("WebSocket get userId error,message: {}", e.getMessage());
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
