package io.tapdata.websocket;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.tapdata.constant.*;
import com.tapdata.entity.AppType;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.ping.PingDto;
import com.tapdata.tm.commons.ping.PingType;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.sdk.util.CloudSignUtil;
import com.tapdata.tm.sdk.util.Version;
import io.tapdata.common.SettingService;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.flow.engine.V2.task.TaskService;
import io.tapdata.websocket.handler.PongHandler;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.socket.*;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.tapdata.websocket.WebSocketEventResult.Type.HANDLE_EVENT_ERROR_RESULT;
import static io.tapdata.websocket.WebSocketEventResult.Type.UNKNOWN_EVENT_RESULT;

/**
 * Websocket事件分发类
 * 主要是监听管理端的推送过来事件
 * <p>
 * sam-20200902改
 * 1. 新增发送消息方法
 * 2. 修改类名: 专门处理与管理端websocket通信(包括收发信息)
 *
 * @author jackin
 */
@Component("managementWebsocketHandler")
@DependsOn("connectorManager")
public class ManagementWebsocketHandler implements WebSocketHandler {

	public static final String WEBSOCKET_CODE_KEY = "websocketCode";
	public static final String WEBSOCKET_MESSAGE_KEY = "websocketMessage";
	public static final int MAX_PING_FAIL_TIME = 3;
	private Logger logger = LogManager.getLogger(ManagementWebsocketHandler.class);

	public static final String URL_PREFIX = "ws://";
	public static final String URL_PREFIX_FOR_SSL = "wss://";

	/**
	 * url 后缀
	 */
	public static final String URL_SUFFIX = "/ws";

	/**
	 * 监听或发送消息的地址
	 */
	public static final String DESTINATION = "/agent";

	private static final Long PING_INTERVAL = 5L;
	/**
	 * websocket接受消息的长度限制：10MB
	 */
	private static final int SESSION_TEXT_LENGTH_LIMIT_BYTE = 10 * 1024 * 1024;

	/**
	 * 与管理端建立连接
	 */
	private ListenableFuture<WebSocketSession> listenableFuture;

	/**
	 * 管理端地址列表
	 */
	private List<String> baseURLs;

	/**
	 * 连接失败后尝试的次数
	 */
	private int retryTime;

	/**
	 * data engine唯一标记，每个agent实例都有一个唯一的id
	 */
	private String agentId;

	/**
	 * 配置中心，获取agent登录用户信息包括access token
	 */
	@Autowired
	private ConfigurationCenter configCenter;

	/**
	 * 管理端操作类
	 */
	@Autowired
	private ClientMongoOperator clientMongoOperator;

	@Autowired
	private TaskService<TaskDto> taskService;

	WebSocketSession session;

	@Autowired
	private SettingService settingService;

	private ScheduledExecutorService healthThreadPool;

	private Set<BeanDefinition> fileDetectorDefinition;
	private final AtomicInteger pingFailTime = new AtomicInteger();
	private boolean wsAlive;
	private String currentWsUrl;

	@PostConstruct
	public void init() {
		this.baseURLs = (List<String>) configCenter.getConfig(ConfigurationCenter.BASR_URLS);
		this.agentId = (String) configCenter.getConfig(ConfigurationCenter.AGENT_ID);
		this.retryTime = (Integer) configCenter.getConfig(ConfigurationCenter.RETRY_TIME);
		this.fileDetectorDefinition = PkgAnnoUtil.getBeanSetWithAnno(Collections.singletonList("io.tapdata.websocket"),
				Collections.singletonList(EventHandlerAnnotation.class));
		healthThreadPool = new ScheduledThreadPoolExecutor(1);
		healthThreadPool.scheduleWithFixedDelay(() -> {
			Thread.currentThread().setName("Management Websocket Health Check");
			if (session == null || !session.isOpen()) {
				logger.info("It is detected that the websocket is not connected, a connection will be created");
				createClients();
			}

			try {
				WebSocketEvent<PingDto> webSocketEvent = new WebSocketEvent<>();
				PingDto pingDto = new PingDto();
				String pingId = UUIDGenerator.uuid();
				pingDto.setPingId(pingId);
				pingDto.setPingType(PingType.WEBSOCKET_HEALTH);
				webSocketEvent.setType("ping");
				webSocketEvent.setData(pingDto);
				sendMessage(new TextMessage(JSONUtil.obj2Json(webSocketEvent)));
				boolean response = PongHandler.handleResponse(pingId, event -> {
					pingFailTime.set(0);
					handleWhenPingSucceed();
				});
				if (!response) {
					if (pingFailTime.incrementAndGet() > MAX_PING_FAIL_TIME) {
						throw new RuntimeException(String.format("No response was received for %s consecutive websocket heartbeats", MAX_PING_FAIL_TIME));
					}
				}
			} catch (Exception e) {
				logger.error("Websocket heartbeat failed, will reconnect. Error: " + e.getMessage(), e);
				handleWhenPingFailed();
				createClients();
			}
		}, 0, PING_INTERVAL, TimeUnit.SECONDS);
	}

	private void handleWhenPingFailed() {
		TapdataTaskScheduler tapdataTaskScheduler = BeanUtil.getBean(TapdataTaskScheduler.class);
		if (null == tapdataTaskScheduler) return;
		tapdataTaskScheduler.startScheduleTask(TapdataTaskScheduler.SCHEDULE_START_TASK_NAME);
		tapdataTaskScheduler.startScheduleTask(TapdataTaskScheduler.SCHEDULE_STOP_TASK_NAME);
	}

	private void handleWhenPingSucceed() {
		TapdataTaskScheduler tapdataTaskScheduler = BeanUtil.getBean(TapdataTaskScheduler.class);
		if (null == tapdataTaskScheduler) return;
		tapdataTaskScheduler.stopScheduleTask(TapdataTaskScheduler.SCHEDULE_START_TASK_NAME);
		tapdataTaskScheduler.stopScheduleTask(TapdataTaskScheduler.SCHEDULE_STOP_TASK_NAME);
	}

	/**
	 * 创建web socket 客户端
	 */
	public synchronized void createClients() {

		// 连接前关闭之前所有的连接
		closeSession();

		if (CollectionUtils.isNotEmpty(baseURLs)) {
			for (String baseURL : baseURLs) {
				connect(baseURL);
				if (session != null && session.isOpen()) {
					break;
				}
			}
		} else {
			logger.error("Connect to management websocket failed, base url(s) is empty");
		}
	}

	private void closeSession() {
		if (session != null) {
			try {
				session.close();
			} catch (IOException ignore) {
			}
		}
	}

	private void connect(String baseURL) {
		currentWsUrl = null;
		try {
			if (StringUtils.startsWithIgnoreCase(baseURL, "http://")) {
				currentWsUrl = baseURL.replace("http://", URL_PREFIX);
			} else if (StringUtils.startsWithIgnoreCase(baseURL, "https://")) {
				currentWsUrl = baseURL.replace("https://", URL_PREFIX_FOR_SSL);
			} else {
				throw new RuntimeException("Connect web socket failed, invalid base url: " + baseURL);
			}
			currentWsUrl = currentWsUrl.replaceAll("/api/", URL_SUFFIX + DESTINATION
					+ "?agentId={agentId}&access_token={access_token}");

			// 临时处理，连接dfs时，去掉"/console"
			if (((AppType) configCenter.getConfig(ConfigurationCenter.APPTYPE)).isDfs()) {
				currentWsUrl = currentWsUrl.replace("/console", "");
			}

			WebSocketClient client = new StandardWebSocketClient();

			currentWsUrl = UriComponentsBuilder.fromUriString(currentWsUrl)
					.buildAndExpand(agentId, configCenter.getConfig(ConfigurationCenter.TOKEN)).encode().toUri().toString();

			if (CloudSignUtil.isNeedSign()) {
				currentWsUrl = CloudSignUtil.getQueryStr("", currentWsUrl);
			}

			String version = Version.get();
			if (org.apache.commons.lang3.StringUtils.isNotEmpty(version)) {
				WebSocketHttpHeaders webSocketHttpHeaders = new WebSocketHttpHeaders();
				webSocketHttpHeaders.add(WebSocketHttpHeaders.USER_AGENT, version);
				this.listenableFuture = client.doHandshake(this, webSocketHttpHeaders, URI.create(UriUtils.decode(currentWsUrl, StandardCharsets.UTF_8)));
			} else {
				this.listenableFuture = client.doHandshake(this, UriUtils.decode(currentWsUrl, StandardCharsets.UTF_8));
			}

			session = listenableFuture.get();
			session.setTextMessageSizeLimit(SESSION_TEXT_LENGTH_LIMIT_BYTE);

			logger.info("Connect to web socket server success, url {}", currentWsUrl);
		} catch (Exception e) {
			logger.error("Create web socket by url {} connection failed {}", currentWsUrl, e.getMessage(), e);
		}
	}

	@Override
	public void afterConnectionEstablished(WebSocketSession session) {
	}

	@Override
	public void handleMessage(WebSocketSession session, WebSocketMessage<?> message) throws Exception {
		logger.debug("Received message {}", message);

		String payload = (String) message.getPayload();

		WebSocketEvent<Map> event = JSONUtil.json2POJO(payload, new TypeReference<WebSocketEvent>() {
		});

		Map eventRequestData = event.getData();
		String messageType = event.getType();
		if (!org.apache.commons.lang3.StringUtils.equals(messageType, TaskDto.SYNC_TYPE_TEST_RUN)) {
			messageType = eventRequestData != null && eventRequestData.containsKey("type") && eventRequestData.get("type") != null ?
					eventRequestData.get("type").toString() : event.getType();
		}

		WebSocketEventHandler<WebSocketEventResult> eventHandler = eventHandler(messageType);

		WebSocketEventResult eventResult;
		if (eventHandler == null) {
			String errorMsg = String.format("Cannot find web socket event handler, type %s", messageType);
			logger.warn(errorMsg);
			eventResult = WebSocketEventResult.handleFailed(UNKNOWN_EVENT_RESULT, errorMsg);
		} else {
			try {
				eventResult = eventHandler.handle(eventRequestData, (data) -> {
					WebSocketEvent<WebSocketEventResult> result = new WebSocketEvent();
					result.setType(event.getType());
					result.setData(data);
					result.setReceiver(event.getSender());
					result.setSender(event.getReceiver());

					JSONUtil.disableFeature(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
					session.sendMessage(new TextMessage(JSONUtil.obj2Json(result)));
				});
			} catch (Exception e) {
				String errorMsg = String.format("Handle websocket event error, event: %s, message: %s",
						event, e.getMessage());
				logger.error(errorMsg, e);
				eventResult = WebSocketEventResult.handleFailed(HANDLE_EVENT_ERROR_RESULT, errorMsg);
			}
		}

		// if eventResult is null, do not reply
		if (eventResult != null) {
			WebSocketEvent<WebSocketEventResult> result = new WebSocketEvent();
			result.setType(event.getType());
			result.setData(eventResult);
			result.setReceiver(event.getSender());
			result.setSender(event.getReceiver());

			logger.info("Processed message result {}.", result);

			JSONUtil.disableFeature(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
			session.sendMessage(
					new TextMessage(JSONUtil.obj2Json(result))
			);
			JSONUtil.enableFeature(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		}
	}

	private WebSocketEventHandler eventHandler(String messageType) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		WebSocketEventHandler eventHandler = null;

		if (StringUtils.isEmpty(messageType)) {
			return null;
		}

		for (BeanDefinition beanDefinition : fileDetectorDefinition) {
			Class<WebSocketEventHandler> aClass = (Class<WebSocketEventHandler>) Class.forName(beanDefinition.getBeanClassName());
			EventHandlerAnnotation[] annotations = aClass.getAnnotationsByType(EventHandlerAnnotation.class);
			if (annotations != null && annotations.length > 0) {
				for (EventHandlerAnnotation annotation : annotations) {
					if (messageType.equals(annotation.type())) {
						eventHandler = aClass.newInstance();
						eventHandler.initialize(taskService, clientMongoOperator, settingService);
					}
				}
			}

			if (eventHandler != null) {
				break;
			}
		}
		return eventHandler;
	}

	@Override
	public void handleTransportError(WebSocketSession session, Throwable exception) {
		logger.error("Web socket handler occur handle transport error {}", exception.getMessage(), exception);
	}

	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) {
		logger.info("Web socket closed, session: {}, status code: {}, reason: {}", currentWsUrl != null ? currentWsUrl : "", closeStatus.getCode(), closeStatus.getReason());
	}

	@Override
	public boolean supportsPartialMessages() {
		return false;
	}

	/**
	 * 发送websocket信息
	 * 随机给已连接的管理端发送
	 *
	 * @param textMessage
	 * @throws IOException
	 */
	public void sendMessage(TextMessage textMessage) throws Exception {
		int failTime = 0;
		while (true) {
			try {
				session.sendMessage(textMessage);
				break;
			} catch (Throwable e) {
				if (++failTime > retryTime) {
					throw e;
				} else {
					logger.warn("Send websocket message failed, fail time: {}, message: {}, err: {}, stack: {}", failTime, textMessage, e.getMessage(), Log4jUtil.getStackString(e));
					Thread.sleep(100L);
					createClients();
				}
			}
		}
	}
}
