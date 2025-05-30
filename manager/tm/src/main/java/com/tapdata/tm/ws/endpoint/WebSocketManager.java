/**
 * @title: WebSocketManager
 * @description:
 * @author lk
 * @date 2021/9/8
 */
package com.tapdata.tm.ws.endpoint;

import com.google.gson.Gson;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.websocket.MessageInfoBuilder;
import com.tapdata.tm.commons.websocket.ReturnCallback;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketEvent;
import com.tapdata.tm.ws.dto.WebSocketInfo;
import com.tapdata.tm.ws.dto.WebSocketResult;
import com.tapdata.tm.ws.handler.*;
import io.tapdata.pdk.apis.exception.TapTestItemException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class WebSocketManager {

	private static final Map<String, WebSocketHandler> webSocketHandlerMap = new ConcurrentHashMap<>();

	private static final Map<String, WebSocketInfo> wsCache = new ConcurrentHashMap<>();

	private static final Map<String, ReturnCallback<?>> resultCallbacks = new ConcurrentHashMap<>();

	public static void addSession(WebSocketInfo webSocketInfo){
		if (webSocketInfo != null && webSocketInfo.getSession() != null && StringUtils.isNotBlank(webSocketInfo.getId())){
			if (!containsSessionId(webSocketInfo.getId())){
				wsCache.put(webSocketInfo.getId(), webSocketInfo);
			}
		}else {
			log.warn("Websocket cache add seesion failed, id can not be blank");
			throw new RuntimeException("Websocket cache add seesion failed, id can not be blank");
		}

	}

	public static void checkAlive(Function<WebSocketInfo, Boolean> consumer) {

		HashSet<String> set = new HashSet<>(wsCache.keySet());
		for (String key : set) {
			Boolean alive = consumer.apply(wsCache.get(key));
			if (!alive) {
				log.warn("Websocket client offline, will be remove session.");
				removeSession(key);
			}
		}
	}


	public static void autoCloseAgentSession() {
		HashSet<String> set = new HashSet<>(wsCache.keySet());
		for (String k : set) {
			WebSocketInfo v = wsCache.get(k);
			if (StringUtils.isNotBlank(v.getAgentId())) {
				try {
					v.getSession().close();
				} catch (IOException e) {
					log.error("close agent websocket error", e);
				} finally {
					removeSession(k);
				}
			}
		}
	}

	/**
	 * 优先匹配 agent id，agent id 为null时匹配 session id
	 * @param uid
	 * @return
	 */
	public static WebSocketInfo getSessionByUid(String uid){
		if (StringUtils.isBlank(uid)){
			log.warn("Websocket cache get seesion failed, uid can not be blank");
			return null;
		}
		// 会存在多个processId的情况，暂取第一个
		//return new HashSet<>(wsCache.entrySet()).stream().filter(entry -> uid.equals(entry.getValue().getUid())).findFirst().map(Map.Entry::getValue).orElse(null);
		return wsCache.values().stream().filter(w -> uid.equals(w.getAgentId()) || uid.equals(w.getId())).findFirst().orElse(null);
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
//			LogsHandler.removeSession(id);
			WatchHandler.removeSession(id);
			NotificationHandler.removeSession(id);
			//DataFlowInsightHandler.removeSession(id);
			EditFlushHandler.removeSession(id);
		}else {
			log.warn("Websocket cache remove seesion skip, id is blank");
		}
	}

	public static boolean containsSession(String id){
		// return !StringUtils.isBlank(id) && new HashSet<>(wsCache.entrySet()).stream().anyMatch(entry -> id.equals(entry.getValue().getUid()));
		return id != null && wsCache.values().stream().anyMatch(m -> id.equals(m.getAgentId()) || id.equals(m.getId()) );
	}

	public static boolean containsSessionId(String id){
		return wsCache.containsKey(id);
	}

	public static void sendMessage(MessageInfo messageInfo) throws IOException {
		sendMessage(messageInfo.getReceiver(), JsonUtil.toJson(messageInfo));
	}

	public static void sendMessage(String id, WebSocketResult result) throws IOException {
		sendMessage(id, JsonUtil.toJson(result));
	}

	public static void sendMessage(String id, String message) throws IOException {
		WebSocketInfo sessionInfo = getSessionByUid(id);
		if(sessionInfo != null && sessionInfo.getSession() != null){
			WebSocketSession session = sessionInfo.getSession();
			message = formatMessageIfNeed(message, session);
			log.debug("WebSocket send message, userId {},id {}, message {}", sessionInfo.getUserId(), id, message);
			sessionInfo.getSession().sendMessage(new TextMessage(message));
		}else{
			log.warn("Can not send message,session does not exist, id: {}", id);
		}
	}

	public static String formatMessageIfNeed(String message, WebSocketSession session) {
		if (StringUtils.isBlank(message)) return message;
		if (message.contains("testConnectionResult")) {
			WebSocketEvent webSocketEvent = JsonUtil.parseJson(message, WebSocketEvent.class);
			Map data = (Map) webSocketEvent.getData();
			Map result = (Map) data.get("result");
			if (null == result) return message;
			Map responseBody = (Map) result.get("response_body");
			List<Map> validateDetails = (List) responseBody.get("validate_details");
			for (Map validateDetail : validateDetails) {
				Object itemException = validateDetail.get("item_exception");
				if (null != itemException) {
					Locale locale = getLocale(session);
					Gson gson = new Gson();
					TapTestItemException itemEx = gson.fromJson(gson.toJson(itemException), TapTestItemException.class);
					String msg = internationalizationMsg(locale, itemEx.getMessage());
					String reason = internationalizationMsg(locale, itemEx.getReason());
					String solution = internationalizationMsg(locale, itemEx.getSolution());
					itemEx.setMessage(msg);
					itemEx.setReason(reason);
					itemEx.setSolution(solution);
					itemException = itemEx;
				}
				validateDetail.put("item_exception", itemException);
			}
			responseBody.put("validate_details", validateDetails);
			result.put("response_body", responseBody);
			data.put("result", result);
			webSocketEvent.setData(data);
			message = JsonUtil.toJson(webSocketEvent);
		}
		return message;
	}
	private static String internationalizationMsg(Locale locale, String message) {
		if (StringUtils.isBlank(message)) return null;
		return MessageUtil.getPdkTestItemMsg(locale, message);
	}
	public static Locale getLocale(WebSocketSession session) {
		if (session != null) {
			HttpHeaders handshakeHeaders = session.getHandshakeHeaders();
			String[] cookies = null;
			String lang = null;
			List<String> cookieString = handshakeHeaders.get("cookie");
			if (cookieString != null) {
				for (String cookie : cookieString) {
					if (null != cookie && cookie.contains("lang")) {
						cookies = cookie.split(";");
						lang = Arrays.stream(cookies).filter(c -> c.contains("lang")).collect(Collectors.toList()).stream().findFirst().orElse(null);
						if (null == lang) continue;
						cookies = lang.split("=");
						if (null == cookies || cookies.length < 2) continue;
						lang = cookies[1];
						break;
					}
				}
			}
			Locale local = null;
			try {
				if(lang != null){
					lang = lang.replaceAll("_","-");
					local = Locale.forLanguageTag(lang);
				}
			} catch (Exception e) {
				log.error(e.getMessage());
			}
			if (local != null) {
				return local;
			}
		}
		return MessageUtil.getLocale();
	}
	public static void sendMessage(String id, com.tapdata.tm.commons.websocket.MessageInfo messageInfo) throws IOException {
		sendMessage(id, messageInfo.toTextMessage());
	}

	public static <T> void sendMessage(String id, com.tapdata.tm.commons.websocket.MessageInfo messageInfo,
									   ReturnCallback<T> returnCallback) throws IOException {
		if (messageInfo.getReqId() == null) {
			messageInfo.setReqId(MessageInfoBuilder.generatorReqId());
		}
		sendMessage(id, messageInfo.toTextMessage());
		cacheReturnCallback(id, messageInfo, returnCallback);
	}

	public static <T> void cacheReturnCallback(String id, com.tapdata.tm.commons.websocket.MessageInfo messageInfo,
											   ReturnCallback<T> returnCallback) {
		if (returnCallback != null) {
			putResultCallback(messageInfo.getReqId(), returnCallback);
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
		if (StringUtils.isNotBlank(type) && handler != null){
			if (webSocketHandlerMap.containsKey(type)){
				log.warn("A message handler of type {}({}) already exists and will be replaced with {}",
						type, webSocketHandlerMap.get(type), handler);
			}
			webSocketHandlerMap.put(type, handler);
		}else {
			log.warn("Websocket handlerMap add handler failed, type can not be blank");
		}

	}

	public static WebSocketHandler getHandler(String type){
		if (StringUtils.isBlank(type)){
			log.warn("Websocket handlerMap get handler failed, type can not be blank");
			return null;
		}
		return webSocketHandlerMap.get(type);
	}

	public static Map<String, WebSocketInfo>  getConnectCount(){

		return wsCache;
	}



	public static <T> void putResultCallback(String reqId, ReturnCallback<T> returnCallback) {
		if (returnCallback != null) {
			resultCallbacks.put(reqId, returnCallback);
		}
	}
	public static boolean containsResultCallback(String reqId) {
		return resultCallbacks.containsKey(reqId);
	}
	public static <T> ReturnCallback getAndRemoveResultCallback(String reqId) {
		if (resultCallbacks.containsKey(reqId)) {
			return resultCallbacks.remove(reqId);
		}
		return null;
	}

	public static List<String> getOnlineAgent() {
		return wsCache.values().stream()
				.filter(m -> StringUtils.isNotBlank(m.getAgentId()))
				.map(WebSocketInfo::getAgentId).collect(Collectors.toList());
	}

	public static List<String> getAllClientId() {
		return wsCache.values().stream().map(webSocketInfo -> webSocketInfo.getAgentId() != null ?
				webSocketInfo.getAgentId() :
				webSocketInfo.getId()).collect(Collectors.toList());
	}
}
