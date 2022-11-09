/**
 * @title: WebSocketManager
 * @description:
 * @author lk
 * @date 2021/9/8
 */
package com.tapdata.tm.ws.endpoint;

import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketInfo;
import com.tapdata.tm.ws.handler.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

@Slf4j
public class WebSocketManager {

	private static final Map<String, List<WebSocketHandler>> webSocketHandlerMap = new ConcurrentHashMap<>();

	private static final Map<String, WebSocketInfo> wsCache = new ConcurrentHashMap<>();

	public static void addSession(String id, String userId, WebSocketSession session){
		if (StringUtils.isNotBlank(id)){
			if (!containsSessionId(session.getId())){
				wsCache.put(session.getId(), new WebSocketInfo(id, userId, session));
			}
		}else {
			log.warn("Websocket cache add seesion failed, id can not be blank");
			throw new RuntimeException("Websocket cache add seesion failed, id can not be blank");
		}

	}

	public static WebSocketInfo getSessionByUid(String uid){
		if (StringUtils.isBlank(uid)){
			log.warn("Websocket cache get seesion failed, uid can not be blank");
			return null;
		}
		// 会存在多个processId的情况，暂取第一个
		return new HashSet<>(wsCache.entrySet()).stream().filter(entry -> uid.equals(entry.getValue().getUid())).findFirst().map(Map.Entry::getValue).orElse(null);
	}

	public static WebSocketInfo getSessionById(String id){
		if (StringUtils.isBlank(id)){
			log.warn("Websocket cache get seesion failed, id can not be blank");
			return null;
		}

		return wsCache.get(id);
	}

	public static void removeSession(String id){
		if (StringUtils.isNotBlank(id)){
			wsCache.remove(id);
			LogsHandler.removeSession(id);
			WatchHandler.removeSession(id);
			NotificationHandler.removeSession(id);
			//DataFlowInsightHandler.removeSession(id);
			EditFlushHandler.removeSession(id);
		}else {
			log.warn("Websocket cache remove seesion skip, id is blank");
		}
	}

	public static boolean containsSession(String id){
		return !StringUtils.isBlank(id) && new HashSet<>(wsCache.entrySet()).stream().anyMatch(entry -> id.equals(entry.getValue().getUid()));
	}

	public static boolean containsSessionId(String id){
		return wsCache.containsKey(id);
	}

	public static void sendMessage(MessageInfo messageInfo) throws IOException {
		sendMessage(messageInfo.getReceiver(), JsonUtil.toJson(messageInfo));
	}

	public static void sendMessage(String id, String message) throws IOException {
		WebSocketInfo sessionInfo = getSessionByUid(id);
		if(sessionInfo != null && sessionInfo.getSession() != null){
			synchronized (sessionInfo.getSession().getId().intern()) {
				sessionInfo.getSession().sendMessage(new TextMessage(message));
			}
		}else{
			log.warn("Can not send message,session does not exist, id: {}", id);
		}
	}

	public static void sendMessageBySessionId(String id, String message) throws IOException {
		if(containsSessionId(id)){
			WebSocketInfo webSocketInfo = getSessionById(id);
			if (webSocketInfo != null){
				webSocketInfo.getSession().sendMessage(new TextMessage(message));
			}
		}else{
			log.warn("Can not send message,session does not exist, id: {}", id);
		}
	}

	public static void addHandler(String type, WebSocketHandler handler){
		if (StringUtils.isNotBlank(type)){
			List<WebSocketHandler> webSocketHandlers = webSocketHandlerMap.get(type);
			if (webSocketHandlers == null){
				webSocketHandlers = new ArrayList<>();
			}
			webSocketHandlers.add(handler);
			webSocketHandlerMap.put(type, webSocketHandlers);
		}else {
			log.warn("Websocket handlerMap add handler failed, type can not be blank");
		}

	}

	public static List<WebSocketHandler> getHandler(String type){
		if (StringUtils.isBlank(type)){
			log.warn("Websocket handlerMap get handler failed, type can not be blank");
			return null;
		}
		return webSocketHandlerMap.get(type);
	}

	public static Map<String, WebSocketInfo>  getConnectCount(){

		return wsCache;
	}


}
