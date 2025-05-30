package io.tapdata.websocket;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.tapdata.constant.*;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.ping.PingDto;
import com.tapdata.tm.commons.ping.PingType;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.sdk.util.CloudSignUtil;
import com.tapdata.tm.sdk.util.Version;
import com.tapdata.tm.worker.WorkerSingletonLock;
import io.tapdata.common.SettingService;
import io.tapdata.common.executor.ThreadFactory;
import io.tapdata.exception.TmUnavailableException;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.flow.engine.V2.task.TaskService;
import io.tapdata.websocket.handler.PongHandler;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.websocket.CloseReason;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tomcat.websocket.WsSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.socket.*;
import org.springframework.web.socket.adapter.NativeWebSocketSession;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriUtils;


import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
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
@DependsOn("tapdataTaskScheduler")
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

	/**
	 * Check ws alive every {PING_INTERVAL} seconds.
	 */
	private static final Long PING_INTERVAL = 10L;
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
	private int retryTime = 20;

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

	private final SessionOption session = new SessionOption();

	@Autowired
	private SettingService settingService;

	private ScheduledExecutorService healthThreadPool;

	private Set<BeanDefinition> fileDetectorDefinition;
	private final AtomicInteger pingFailTime = new AtomicInteger();
	private String currentWsUrl;

	private ThreadPoolExecutor websocketHandleMessageThreadPoolExecutor;

	@PostConstruct
	public void init() {
		this.baseURLs = (List<String>) configCenter.getConfig(ConfigurationCenter.BASR_URLS);
		this.agentId = (String) configCenter.getConfig(ConfigurationCenter.AGENT_ID);
		this.retryTime = (Integer) configCenter.getConfig(ConfigurationCenter.RETRY_TIME);
		this.fileDetectorDefinition = PkgAnnoUtil.getBeanSetWithAnno(Collections.singletonList("io.tapdata.websocket"),
				Collections.singletonList(EventHandlerAnnotation.class));
		healthThreadPool = new ScheduledThreadPoolExecutor(1);
		int corePoolSize = Runtime.getRuntime().availableProcessors() * 2;
		int maximumPoolSize = 32;
		if (maximumPoolSize < corePoolSize) {
			maximumPoolSize = corePoolSize;
		}
		this.websocketHandleMessageThreadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 1L, TimeUnit.SECONDS,
				new LinkedBlockingDeque<>(100),
				new ThreadFactory("Thread-websocket-handle-message-"),
				(runnable, executor) -> {
					logger.error("Thread is rejected, runnable {} pool {}", runnable, executor);
				});
		healthThreadPool.scheduleWithFixedDelay(() -> {
			try {
				Thread.currentThread().setName("Management Websocket Health Check");

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
					if (!response && pingFailTime.incrementAndGet() > MAX_PING_FAIL_TIME ) {
						session.release();
						throw new RuntimeException(String.format("No response was received for %s consecutive websocket heartbeats", MAX_PING_FAIL_TIME));
					}
				} catch (Exception e) {
					logger.error("Websocket heartbeat failed, will reconnect. Error: " + e.getMessage(), e);
				}
			} catch (Exception e) {
				logger.error("Websocket heartbeat failed, will reconnect after {}s. Error: {}", PING_INTERVAL, e.getMessage(), e);
			}
		}, 0, PING_INTERVAL, TimeUnit.SECONDS);
	}

	@PreDestroy
	public void destroy() {
		this.websocketHandleMessageThreadPoolExecutor.shutdown();
	}

	private void handleWhenPingFailed() {
		DebounceUtil.debounce("StopTaskSchedulerOnWSDisconnect", 10000, () -> {
			TapdataTaskScheduler tapdataTaskScheduler = BeanUtil.getBean(TapdataTaskScheduler.class);
			if (null == tapdataTaskScheduler) return;
			tapdataTaskScheduler.startScheduleTask(TapdataTaskScheduler.SCHEDULE_START_TASK_NAME);
			tapdataTaskScheduler.startScheduleTask(TapdataTaskScheduler.SCHEDULE_STOP_TASK_NAME);
		});
	}

	private void handleWhenPingSucceed() {
		DebounceUtil.debounce("StartTaskSchedulerOnWSConnected", 10000, () -> {
			TapdataTaskScheduler tapdataTaskScheduler = BeanUtil.getBean(TapdataTaskScheduler.class);
			if (null == tapdataTaskScheduler) return;
			tapdataTaskScheduler.stopScheduleTask(TapdataTaskScheduler.SCHEDULE_START_TASK_NAME);
			tapdataTaskScheduler.stopScheduleTask(TapdataTaskScheduler.SCHEDULE_STOP_TASK_NAME);
		});
	}

	protected void connect(String baseURL) {
		currentWsUrl = null;
		try {
			if (StringUtils.startsWithIgnoreCase(baseURL, "http://")) {
				currentWsUrl = baseURL.replace("http://", URL_PREFIX);
			} else if (StringUtils.startsWithIgnoreCase(baseURL, "https://")) {
				currentWsUrl = baseURL.replace("https://", URL_PREFIX_FOR_SSL);
			} else {
				throw new RuntimeException("Connect web socket failed, invalid base url: " + baseURL);
			}
			currentWsUrl = currentWsUrl.replace("/api/", URL_SUFFIX + DESTINATION
					+ "?agentId={agentId}&access_token={access_token}");


			WebSocketClient client = ManagementWebsocketHandler.createWebSocketClient();

			currentWsUrl = UriComponentsBuilder.fromUriString(currentWsUrl)
					.buildAndExpand(agentId, configCenter.getConfig(ConfigurationCenter.TOKEN)).encode().toUri().toString();

			currentWsUrl = WorkerSingletonLock.addTag2WsUrl(currentWsUrl);
			if (CloudSignUtil.isNeedSign()) {
				currentWsUrl = CloudSignUtil.getQueryStr("", currentWsUrl);
			}

			String version = Version.get();
			if (org.apache.commons.lang3.StringUtils.isNotEmpty(version)) {
				WebSocketHttpHeaders webSocketHttpHeaders = new WebSocketHttpHeaders();
				webSocketHttpHeaders.add(HttpHeaders.USER_AGENT, version);
				this.listenableFuture = client.doHandshake(this, webSocketHttpHeaders,
						URI.create(currentWsUrl));
			} else {
				this.listenableFuture = client.doHandshake(this, UriUtils.decode(currentWsUrl, StandardCharsets.UTF_8));
			}

			session.setSession(listenableFuture.get());
			logger.info("Connect to web socket server success, url {}", currentWsUrl);
		} catch (InterruptedException interruptedException) {
			Thread.currentThread().interrupt();
			logger.warn("Connect to web socket Thread interrupted,Thread name:{}", Thread.currentThread().getName());
		}
		catch (Exception e) {
			logger.error("Create web socket by url {} connection failed {}", currentWsUrl, e.getMessage(), e);
		}
	}

	@Override
	public void afterConnectionEstablished(WebSocketSession session) {
		handleWhenPingSucceed();
	}

	@Override
	public void handleMessage(final WebSocketSession session, final WebSocketMessage<?> message) throws Exception {
		Runnable runnable = () -> {
			try {
				logger.debug("Received message {}", message);

				String payload = (String) message.getPayload();

				WebSocketEvent<Map<Object,Object>> event = JSONUtil.json2POJO(payload, new TypeReference<WebSocketEvent<Map<Object,Object>>>() {
				});

				Map<Object,Object> eventRequestData = event.getData();
				String messageType = event.getType();
				if (!StringUtils.equals(messageType, TaskDto.SYNC_TYPE_TEST_RUN)) {
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
							WebSocketEvent<WebSocketEventResult> result = new WebSocketEvent<>();
							result.setType(event.getType());
							result.setData(data);
							result.setReceiver(event.getSender());
							result.setSender(event.getReceiver());

							JSONUtil.disableFeature(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
							sendMessage(new TextMessage(JSONUtil.obj2Json(result)));
						});
					} catch (Exception e) {
						String errorMsg;
						if (TmUnavailableException.isInstance(e)) {
							errorMsg = String.format("Handle websocket event error because TM unavailable, event: %s, message: %s", event, e.getMessage());
							logger.warn(errorMsg);
						} else {
							errorMsg = String.format("Handle websocket event error, event: %s, message: %s", event, e.getMessage());
							logger.error(errorMsg, e);
						}
						eventResult = WebSocketEventResult.handleFailed(HANDLE_EVENT_ERROR_RESULT, errorMsg);
					}
				}

				// if eventResult is null, do not reply
				if (eventResult != null) {
					WebSocketEvent<WebSocketEventResult> result = new WebSocketEvent<>();
					result.setType(event.getType());
					result.setData(eventResult);
					result.setReceiver(event.getSender());
					result.setSender(event.getReceiver());

					logger.info("Processed message result {}.", result);

					JSONUtil.disableFeature(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
					sendMessage(new TextMessage(JSONUtil.obj2Json(result)));
					JSONUtil.enableFeature(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
				}
			} catch (Exception e) {
				String errorMsg = String.format("Handle websocket event error, message: %s, error: %s", message, e.getMessage());
				logger.error(errorMsg, e);
			}
		};

		websocketHandleMessageThreadPoolExecutor.execute(runnable);
	}

	private WebSocketEventHandler<WebSocketEventResult> eventHandler(String messageType) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		WebSocketEventHandler<WebSocketEventResult> eventHandler = null;

		if (StringUtils.isEmpty(messageType)) {
			return null;
		}

		for (BeanDefinition beanDefinition : fileDetectorDefinition) {
			Class<WebSocketEventHandler<WebSocketEventResult>> aClass = (Class<WebSocketEventHandler<WebSocketEventResult>>) Class.forName(beanDefinition.getBeanClassName());
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
		this.session.release();
		handleWhenPingFailed();
	}

	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) {
		logger.info("Web socket closed, session: {}, status code: {}, reason: {}", currentWsUrl != null ? currentWsUrl : "", closeStatus.getCode(), closeStatus.getReason());
		this.session.release(closeStatus);
		handleWhenPingFailed();
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
	public void sendMessage(TextMessage textMessage) throws RuntimeException {
		session.sendMessage(textMessage);
	}

	public static WebSocketClient createWebSocketClient() {
		return new StandardWebSocketClient();
	}

	protected class SessionOption implements AutoCloseable {
		private WebSocketSession session;

		protected synchronized boolean isOpen() {
			return null != session && session.isOpen();
		}

		synchronized void setSession(WebSocketSession session) {
			release();
			this.session = session;
			this.session.setTextMessageSizeLimit(SESSION_TEXT_LENGTH_LIMIT_BYTE);
		}

		protected synchronized void connect() {
			if (isOpen()) return;

			// 连接前关闭之前所有的连接
			release();
			List<String> urLs = getBaseURLs();
			if (CollectionUtils.isNotEmpty(urLs)) {
				for (String baseURL : urLs) {
					getManagementWebsocketHandler().connect(baseURL);
					if (isOpen()) break;
				}
				if (!isOpen()) {
					throw new RuntimeException("Send websocket message failed, can not connected any one of TM before send message");
				}
			} else {
				throw new RuntimeException("Connect to management websocket failed, base url(s) is empty");
			}
		}

		protected synchronized void release() {
			release(null);
		}
		synchronized void release(CloseStatus closeStatus) {
			if (null != session) {
				try {
					if (null != closeStatus && session instanceof NativeWebSocketSession) {
						WsSession nativeSession = ((NativeWebSocketSession) session).getNativeSession(WsSession.class);
						if (null != nativeSession) {
							CloseReason.CloseCode closeCode = CloseReason.CloseCodes.getCloseCode(closeStatus.getCode());
							CloseReason closeReason = new CloseReason(closeCode, null);
							nativeSession.onClose(closeReason);
						}
					}
					session.close();
					session = null;
				} catch (IOException e) {
					logger.warn("Close session('{}':{}) failed: {}", session.getId(), session.getUri(), e.getMessage(), e);
				}
			}
		}

		synchronized void sendMessage(TextMessage textMessage) {
			long now = System.currentTimeMillis();
			try {
				int failTime = 0;
				while (!Thread.interrupted()) {
					try {
						if (!isOpen()) {
							release();
							connect();
						}

						session.sendMessage(textMessage);
						break;
					} catch (Exception e) {
						if (++failTime > retryTime) {
							throw new RuntimeException("Retried sending " + failTime + " times, duration " + (System.currentTimeMillis() - now) + ", but all failed, cancel retry.", e);
						}
						logger.warn("Send websocket message failed, fail time: {}, message: {}, err: {}, stack: {}", failTime, textMessage, e.getMessage(), Log4jUtil.getStackString(e));
						release();
					}

					wait(500L);
				}
			} catch (InterruptedException e) {
				logger.warn("Waiting to be interrupted, use {}ms: {}", (System.currentTimeMillis() - now), textMessage, e);
				Thread.currentThread().interrupt();
			}
		}

		@Override
		public synchronized void close() throws Exception {
			release();
		}
		protected ManagementWebsocketHandler getManagementWebsocketHandler() {
			return ManagementWebsocketHandler.this;
		}
		protected List<String> getBaseURLs() {
			return baseURLs;
		}
	}
}
