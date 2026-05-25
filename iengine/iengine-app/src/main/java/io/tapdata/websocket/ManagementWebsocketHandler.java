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
import io.tapdata.pdk.core.utils.CommonUtils;
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
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.event.EventListener;
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
import java.util.concurrent.atomic.AtomicBoolean;
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
	public static final int MAX_PING_FAIL_TIME = (int) CommonUtils.getPropertyLong("WS_MAX_PING_FAIL", 2L);
	private static final long HANDSHAKE_TIMEOUT_MS = CommonUtils.getPropertyLong("WS_HANDSHAKE_TIMEOUT_MS", 5000L);
	private static final long RECONNECT_TOTAL_BUDGET_MS = CommonUtils.getPropertyLong("WS_RECONNECT_TOTAL_BUDGET_MS", 30_000L);
	private static final long FALLBACK_DEBOUNCE_MS = CommonUtils.getPropertyLong("WS_FALLBACK_DEBOUNCE_MS", 2_000L);
	/**
	 * Grace sleep applied after a failed handshake when only one base URL is configured
	 * (nginx single-URL deployment). Gives nginx time to mark the dead upstream as down
	 * via fail_timeout before the next handshake attempt routes through the same proxy.
	 */
	private static final long WS_NGINX_FAIL_TIMEOUT_MS = CommonUtils.getPropertyLong("WS_NGINX_FAIL_TIMEOUT_MS", 1500L);
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
	 * Check ws alive every {PING_INTERVAL} seconds. Tightened from 10s → 5s so a failed-node
	 * detection lands in 5s × MAX_PING_FAIL = 10s, comfortably inside the 25s lastHeartbeat
	 * window even when nginx fronts a dead TM upstream. Configurable via WS_PING_INTERVAL_S.
	 */
	private static final Long PING_INTERVAL = CommonUtils.getPropertyLong("WS_PING_INTERVAL_S", 5L);
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
	private ExecutorService reconnectExecutor;

	private Set<BeanDefinition> fileDetectorDefinition;
	private final AtomicInteger pingFailTime = new AtomicInteger();
	// Static so that SessionOption (inner class, accessed in unit tests via Mockito doCallRealMethod
	// without the synthetic outer reference) can read it without NPE. Singleton bean — behavior unchanged.
	static final AtomicInteger lastSuccessfulUrlIndex = new AtomicInteger(0);
	private final AtomicBoolean reconnectInFlight = new AtomicBoolean(false);
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
		this.reconnectExecutor = Executors.newSingleThreadExecutor(new ThreadFactory("Thread-ws-reconnect-"));
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
					if (!response && pingFailTime.incrementAndGet() >= MAX_PING_FAIL_TIME) {
						pingFailTime.set(0);
						logger.warn("No response was received for {} consecutive websocket heartbeats, triggering async reconnect", MAX_PING_FAIL_TIME);
						triggerReconnect();
					}
				} catch (Exception e) {
					logger.error("Websocket heartbeat failed, will reconnect. Error: " + e.getMessage(), e);
				}
			} catch (Exception e) {
				logger.error("Websocket heartbeat failed, will reconnect after {}s. Error: {}", PING_INTERVAL, e.getMessage(), e);
			}
		}, 0, PING_INTERVAL, TimeUnit.SECONDS);
	}

	/**
	 * 启动期竞态修复：在 WS 握手完成前（最长 HANDSHAKE_TIMEOUT_MS=5000ms），
	 * TM 的 REST 心跳已经把本引擎标记为存活（约 T+5s），可能会派发任务过来。
	 * 此时立刻启动 HTTP 轮询兜底，确保握手窗口内被分配到的 WAIT_RUN 任务
	 * 能被本引擎主动拉起（{@link TapdataTaskScheduler#scheduledTask()} 每 1s 轮询
	 * agentId+status=WAIT_RUN）。WS 稳定后，{@link #handleWhenPingSucceed()}
	 * 会在 10s 防抖后自动把它们停掉，回到默认的 WS 驱动模式。
	 *
	 * 使用 ApplicationReadyEvent 而非 @PostConstruct：虽然本类已通过
	 * @DependsOn("tapdataTaskScheduler") 保证依赖 bean 先就绪，但
	 * ApplicationReadyEvent 在整个 Spring 上下文完全初始化后才触发，
	 * 更稳妥，且仍远早于 TM 派发任务到达本引擎（TM 看到
	 * worker.ping_time 需至少 5s）。
	 */
	@EventListener(ApplicationReadyEvent.class)
	public void startHttpFallbackBeforeWsReady() {
		TapdataTaskScheduler scheduler = BeanUtil.getBean(TapdataTaskScheduler.class);
		if (scheduler == null) {
			logger.warn("TapdataTaskScheduler bean not available at ApplicationReadyEvent; HTTP fallback not started");
			return;
		}
		scheduler.startScheduleTask(TapdataTaskScheduler.SCHEDULE_START_TASK_NAME);
		scheduler.startScheduleTask(TapdataTaskScheduler.SCHEDULE_STOP_TASK_NAME);
		logger.info("Started HTTP task polling fallback at engine boot; will be stopped after WS stabilises");
	}

	@PreDestroy
	public void destroy() {
		this.websocketHandleMessageThreadPoolExecutor.shutdown();
		if (this.reconnectExecutor != null) {
			this.reconnectExecutor.shutdownNow();
		}
	}

	/**
	 * Trigger an asynchronous reconnect. De-duped via {@link #reconnectInFlight} so that
	 * concurrent heartbeat-fail / send-fail paths cannot enqueue more than one outstanding
	 * reconnect attempt at a time. The heartbeat thread must never call session.release() /
	 * session.connect() directly — handshake timeouts can take seconds and must not stall
	 * the health check cadence.
	 */
	private void triggerReconnect() {
		if (!reconnectInFlight.compareAndSet(false, true)) {
			logger.debug("Reconnect already in flight, skipping duplicate trigger");
			return;
		}
		if (reconnectExecutor == null || reconnectExecutor.isShutdown()) {
			reconnectInFlight.set(false);
			return;
		}
		reconnectExecutor.submit(() -> {
			try {
				session.release();
				session.connect();
			} catch (Exception e) {
				logger.warn("Async reconnect attempt failed: {}", e.getMessage());
			} finally {
				reconnectInFlight.set(false);
			}
		});
	}

	private void handleWhenPingFailed() {
		DebounceUtil.debounce("StopTaskSchedulerOnWSDisconnect", (int) FALLBACK_DEBOUNCE_MS, () -> {
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

			session.setSession(listenableFuture.get(HANDSHAKE_TIMEOUT_MS, TimeUnit.MILLISECONDS));
			logger.info("Connect to web socket server success, url {}", currentWsUrl);
		} catch (InterruptedException interruptedException) {
			Thread.currentThread().interrupt();
			if (this.listenableFuture != null) {
				this.listenableFuture.cancel(true);
			}
			logger.warn("Connect to web socket Thread interrupted,Thread name:{}", Thread.currentThread().getName());
		} catch (TimeoutException timeoutException) {
			if (this.listenableFuture != null) {
				this.listenableFuture.cancel(true);
			}
			logger.warn("Connect to web socket {} timed out after {}ms, will try next URL", currentWsUrl, HANDSHAKE_TIMEOUT_MS);
		}
		catch (Exception e) {
			if (this.listenableFuture != null) {
				this.listenableFuture.cancel(true);
			}
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
		// Eager reconnect: don't wait for the next ping cycle (5s) to discover the broken socket.
		// triggerReconnect() de-dupes via reconnectInFlight, so concurrent paths cannot stack.
		triggerReconnect();
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
			if (CollectionUtils.isEmpty(urLs)) {
				throw new RuntimeException("Connect to management websocket failed, base url(s) is empty");
			}
			long deadline = System.currentTimeMillis() + RECONNECT_TOTAL_BUDGET_MS;
			int size = urLs.size();
			int startIdx = Math.floorMod(lastSuccessfulUrlIndex.get(), size);
			for (int i = 0; i < size; i++) {
				if (System.currentTimeMillis() >= deadline) {
					logger.warn("Reconnect total budget {}ms exhausted after trying {} URL(s)", RECONNECT_TOTAL_BUDGET_MS, i);
					break;
				}
				int idx = (startIdx + i) % size;
				getManagementWebsocketHandler().connect(urLs.get(idx));
				if (isOpen()) {
					lastSuccessfulUrlIndex.set(idx);
					return;
				}
			}
			// Single-URL nginx mode: if the for-loop above only had one URL to try and the
			// handshake failed, the next call site will retry through the same nginx proxy
			// almost immediately. Sleep WS_NGINX_FAIL_TIMEOUT_MS to give nginx a chance to
			// mark the dead upstream as down (fail_timeout) before our caller retries.
			if (size == 1 && WS_NGINX_FAIL_TIMEOUT_MS > 0 && System.currentTimeMillis() < deadline) {
				try {
					this.wait(WS_NGINX_FAIL_TIMEOUT_MS);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
				}
			}
			throw new RuntimeException("Send websocket message failed, can not connect any of TM URLs: " + urLs);
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
						// Eager async reconnect alongside the synchronous retry below — gives the
						// background reconnect thread a head start so the next iteration's connect()
						// may already find an open session. Idempotent via reconnectInFlight.
						triggerReconnect();
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
