package io.tapdata.wsserver.channels.gateway;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.MainMethod;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.ClassAnnotationHandler;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.modules.api.net.data.*;
import io.tapdata.modules.api.net.entity.NodeRegistry;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.message.TapEntity;
import io.tapdata.modules.api.net.service.node.NodeHealthService;
import io.tapdata.modules.api.net.service.node.NodeRegistryService;
import io.tapdata.modules.api.net.service.node.connection.NodeConnection;
import io.tapdata.modules.api.net.service.node.connection.NodeConnectionFactory;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.utils.AnnotationUtils;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.IPHolder;
import io.tapdata.wsserver.channels.error.WSErrors;
import io.tapdata.wsserver.channels.gateway.data.UserChannel;
import io.tapdata.wsserver.channels.gateway.modules.GatewayChannelModule;
import io.tapdata.wsserver.channels.health.HealthWeightListener;
import io.tapdata.wsserver.channels.health.NodeHealthManager;
import io.tapdata.wsserver.channels.websocket.WebSocketManager;
import io.tapdata.wsserver.channels.websocket.utils.ValidateUtils;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.queue.SingleThreadBlockingQueue;
import io.tapdata.pdk.core.utils.state.StateMachine;
import org.apache.commons.lang3.StringUtils;
import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.tapdata.entity.simplify.TapSimplify.map;

@MainMethod(value = "start", order = 100000)
@Bean
public class GatewaySessionManager implements HealthWeightListener, MemoryFetcher {
    public final String TAG = GatewaySessionManager.class.getSimpleName();

    private String jwtKey = "adfJ#R$LKLKFJERL(*$#@FD";
    private Integer jwtExpireSeconds = 60 * 60 * 12;

    private Long sessionExpireCheckPeriodSeconds = 30L;
    private Long sessionInactiveExpireTime = TimeUnit.SECONDS.toMillis(10);
    private String authorisedExpression = ".*_.*Apis_.*";

    @Bean
    private NodeRegistryService nodeRegistryService;
    @Bean
    private NodeHealthService nodeHealthService;

    @Bean
    private NodeHealthManager nodeHealthManager;

    public final String JWT_FIELD_USER_ID = "u";

    public static final String JWT_FIELD_AUTHORISED_SERVICES = "as";
    public static final String JWT_FIELD_DEVICE_TOKEN_CRC = "d";
    public static final String JWT_FIELD_TERMINAL = "t";
    public static final String JWT_FIELD_ACTIVE_LOGIN = "a";

    @Bean
    private GatewaySessionAnnotationHandler gatewaySessionAnnotationHandler;
    @Bean
    private WebSocketManager webSocketManager;
    private final Object userLock = new Object();

    private int roomSessionManagerQueueCapacity = 10000;
    private int  roomSessionManagerCoreSize = 100;
    private int roomSessionManagerMaximumPoolSize = 500;
    private int roomSessionManagerKeepAliveSeconds = 120;
    public final String KEY_USER = "user";

    public static final int STATE_NONE = 1;
    public static final int STATE_SCAN_GATEWAY_SESSION = 10;
    public static final int STATE_STARTED = 20;
    public static final int STATE_PAUSED = 30;
    public static final int STATE_TERMINATED = 120;
    private StateMachine<Integer, GatewaySessionManager> stateMachine;
//    StateOperateRetryHandler reportRetryHandler
//    ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("RoomSessionManager-%d").build())
    private ThreadPoolExecutor threadPoolExecutor;

    private int maxChannels = 8000;
    @Bean
    private JsonParser jsonParser;

    OutgoingMessageFilter broadcastOutgoingMessageFilter;

    private ConcurrentHashMap<String, GatewaySessionHandler> userIdGatewaySessionHandlerMap = new ConcurrentHashMap<>();
    //无论作为User进入还是Viewer进入都会在这个Map里， 如果通道断之后， 会从这个Map里删除
    private ConcurrentHashMap<String, UserChannel> userIdGatewayChannelMap = new ConcurrentHashMap<>();
    private Map<String, String> tokenUserIdMap = new ConcurrentHashMap<>();
    private volatile int roomCounter = 0;

    private ConcurrentHashMap<String, SingleThreadBlockingQueue<UserAction>> userIdSingleThreadMap = new ConcurrentHashMap<>();

    @Bean
    private GatewayChannelModule gatewayChannelModule;

    @Bean
    private NodeConnectionFactory nodeConnectionFactory;

    private NodeRegistry nodeRegistry;
    public void start() {
        threadPoolExecutor = new ThreadPoolExecutor(roomSessionManagerCoreSize, roomSessionManagerMaximumPoolSize, roomSessionManagerKeepAliveSeconds, TimeUnit.SECONDS, new LinkedBlockingQueue<>(roomSessionManagerQueueCapacity), new ThreadFactoryBuilder().setNameFormat("GatewaySessionManager-%d").build());
        threadPoolExecutor.allowCoreThreadTimeOut(true);

        stateMachine = new StateMachine<>(GatewaySessionManager.class.getSimpleName() + "#", STATE_NONE, this);
        stateMachine
                .configState(STATE_NONE, stateMachine.execute().nextStates(STATE_SCAN_GATEWAY_SESSION))
                .configState(STATE_SCAN_GATEWAY_SESSION, stateMachine.execute(this::handleScanRoomSessionState).nextStates(STATE_STARTED, STATE_TERMINATED))
                .configState(STATE_STARTED, stateMachine.execute(this::handlerStartedState).nextStates(STATE_PAUSED, STATE_TERMINATED))
                .configState(STATE_PAUSED, stateMachine.execute().nextStates(STATE_STARTED, STATE_TERMINATED))
                .configState(STATE_TERMINATED, stateMachine.execute());

        stateMachine.gotoState(STATE_SCAN_GATEWAY_SESSION, "Start scanning GatewaySessionHandler class");

        PDKIntegration.registerMemoryFetcher(GatewaySessionManager.class.getSimpleName(), this);
    }

    private void handlerStartedState(GatewaySessionManager gatewaySessionManager, StateMachine<Integer, GatewaySessionManager> integerGatewaySessionManagerStateMachine) {
        webSocketManager.start();

        ExecutorsManager.getInstance().getScheduledExecutorService().scheduleAtFixedRate(() -> {
            try {
                List<GatewaySessionHandler> deletedHandlers = new ArrayList<>();
                Collection<GatewaySessionHandler> gatewaySessionHandlers = userIdGatewaySessionHandlerMap.values();
                for (GatewaySessionHandler gatewaySessionHandler : gatewaySessionHandlers) {
                    if (System.currentTimeMillis() - gatewaySessionHandler.getTouch() > sessionInactiveExpireTime) {
                        deletedHandlers.add(gatewaySessionHandler);
                    }
                }
                for (GatewaySessionHandler gatewaySessionHandler : deletedHandlers) {
                    if (System.currentTimeMillis() - gatewaySessionHandler.getTouch() > sessionInactiveExpireTime) {
                        TapLogger.debug(TAG, "GatewaySessionHandler expired, will be removed, id {}. last touch time {} sessionInactiveExpireTime {}", gatewaySessionHandler.getId(), new Date(gatewaySessionHandler.getTouch()), sessionInactiveExpireTime);
                        closeSession(gatewaySessionHandler.getId());
                    }
                }
            } catch(Throwable t) {
                TapLogger.error(TAG, "Session expiration handle failed " + t.getMessage());
            }
        }, sessionExpireCheckPeriodSeconds, sessionExpireCheckPeriodSeconds, TimeUnit.SECONDS);

        IPHolder ipHolder = new IPHolder();
        ipHolder.init();
        int httpPort = CommonUtils.getPropertyInt("tapdata_proxy_server_port", 3000); //TODO should read from TM config.
        nodeRegistry = new NodeRegistry().ips(ipHolder.getIps()).httpPort(httpPort).wsPort(webSocketManager.getWebSocketProperties().getPort()).type("proxy").time(System.currentTimeMillis());
        CommonUtils.setProperty("tapdata_node_id", nodeRegistry.id());
        nodeRegistryService.save(nodeRegistry);
        
        TapLogger.debug(TAG, "Register node {}", nodeRegistry);
        nodeHealthManager.setNewNodeConsumer(addedNodeRegistry -> nodeConnectionFactory.getNodeConnection(addedNodeRegistry.id()));
        nodeHealthManager.setDeleteNodeConsumer(deletedNodeRegistry -> {
            NodeConnection nodeConnection = nodeConnectionFactory.removeNodeConnection(deletedNodeRegistry.id());
            if(nodeConnection != null)
                nodeConnection.close();
        });
        nodeHealthManager.start(this);
    }

    private void handleScanRoomSessionState(GatewaySessionManager gatewaySessionManager, StateMachine<Integer, GatewaySessionManager> integerGatewaySessionManagerStateMachine) {
        ConfigurationBuilder builder = new ConfigurationBuilder()
                .addScanners(new TypeAnnotationsScanner())
//                .forPackages(this.scanPackages)
                .addClassLoader(this.getClass().getClassLoader());
        String scanPackage = CommonUtils.getProperty("gateway_scan_package", "io.tapdata.proxy");
        String[] packages = scanPackage.split(",");

        builder.forPackages(packages);
        Reflections reflections = new Reflections(builder);

        TapLogger.debug(TAG, "Start scanning gateway session classes");
        AnnotationUtils.runClassAnnotationHandlers(reflections, new ClassAnnotationHandler[]{
                gatewaySessionAnnotationHandler
        }, TAG);

        if (gatewaySessionAnnotationHandler.isEmpty()) {
            this.stateMachine.gotoState(STATE_TERMINATED, "No gatewaySessionHandler found");
        } else {
            this.stateMachine.gotoState(STATE_STARTED, "Scanned gatewaySessionHandler " + gatewaySessionAnnotationHandler);
        }
    }

    public void stop() {
        TapLogger.debug(TAG, "GatewaySessionManager is stopping...");
        long time = System.currentTimeMillis();
        Collection<String> userIds = new ArrayList<String>(userIdGatewaySessionHandlerMap.keySet());
        for (String userId : userIds) {
            closeSession(userId);
        }
        webSocketManager.stop();
        TapLogger.debug(TAG, "RoomSessionManager stopped, takes {}", System.currentTimeMillis() - time);
    }

    public SingleThreadBlockingQueue<UserAction> removeUserActionQueue(String userId) {
        SingleThreadBlockingQueue<UserAction> queue = userIdSingleThreadMap.remove(userId);
//        queue.queue.clear()
        return queue;
    }

    private SingleThreadBlockingQueue<UserAction> getUserActionQueue(String userId) {
        SingleThreadBlockingQueue<UserAction> queue = userIdSingleThreadMap.get(userId);
        if (queue == null) {
            UserActionHandler userActionHandler = new UserActionHandler();
            InstanceFactory.injectBean(userActionHandler);
            queue = new SingleThreadBlockingQueue<UserAction>("UserActionQueue_" + userId)
                    .withErrorHandler(userActionHandler)
                    .withHandler(userActionHandler)
                    .withExecutorService(threadPoolExecutor)
                    .withHandleSize(100)
                    .withMaxSize(500)
                    .withMaxWaitMilliSeconds(0)
                    .start();
            SingleThreadBlockingQueue<UserAction> old = userIdSingleThreadMap.putIfAbsent(userId, queue);
            if (old != null) {
                queue.stop();
                queue = old;
            }
         }
        return queue;
    }

    public void replaceChannel(GatewaySessionHandler gatewaySessionHandler, String authorisedExpression, String deviceToken, Integer terminal, int code) {
        closeChannel(gatewaySessionHandler.getId(), code);
        UserChannel userChannel = gatewaySessionHandler.getUserChannel();
        userChannel.setTerminal(terminal);
        userChannel.setDeviceToken(deviceToken);
        userChannel.setUpdateTime(System.currentTimeMillis());
        userChannel.setAuthorisedExpression(authorisedExpression);
    }

    private static void verifyAuthorisedToken(GatewaySessionHandler gatewaySessionHandler, String authorisedToken) {
        try {
            gatewaySessionHandler.verifyAuthorisedToken(authorisedToken);
        } catch (Throwable throwable) {
            if (throwable instanceof CoreException) {
                throw throwable;
            }
            throw new CoreException(WSErrors.ERROR_VERIFY_AUTHORISED_TOKEN_FAILED, "Verify authorised token " + authorisedToken + " failed, " + throwable.getMessage());
        }
    }

    public void reLogin(GatewaySessionHandler gatewaySessionHandler, String authorisedExpression, String deviceToken, Integer terminal, Boolean activeLogin, String authorisedToken) {
        if (gatewaySessionHandler.getUserChannel() == null)
            throw new CoreException(WSErrors.ERROR_USER_CHANNEL_NULL, "User channel is null while preCreateGatewaySessionHandler userId $gatewaySessionHandler.id authorisedExpression $authorisedExpression deviceToken $deviceToken terminal $terminal activeLogin $activeLogin");

        verifyAuthorisedToken(gatewaySessionHandler, authorisedToken);

        if (authorisedExpression == null)
            authorisedExpression = this.authorisedExpression;

        if (activeLogin) { //用户主动登录时， 说明用户是点击登录后进入的， 所以无论如何要替换通道
            replaceChannel(gatewaySessionHandler, authorisedExpression, deviceToken, terminal, WSErrors.ERROR_CHANNEL_KICKED_BY_DEVICE);
        } else if (Objects.equals(gatewaySessionHandler.getUserChannel().getDeviceToken(), deviceToken)) {
            //当用户是被动登录时， 因为断网重新登录的时候为被动登录， 如果deviceToken是一样的， 也替换通道
            replaceChannel(gatewaySessionHandler, authorisedExpression, deviceToken, terminal, WSErrors.ERROR_CHANNEL_KICKED_BY_CONCURRENT);
        } else {//当用户是被动登录时， 因为断网重新登录的时候为被动登录， 如果deviceToken是不一样的， 说明这是旧设备登录， 不应该允许
            throw new CoreException(WSErrors.ERROR_LOGIN_FAILED_DEVICE_TOKEN_CHANGED, "Rejected because this is old device login, old deviceToken {} current {} userId {} authorisedExpression {} terminal {} activeLogin {}", deviceToken, gatewaySessionHandler.getUserChannel().getDeviceToken(), gatewaySessionHandler.getId(), authorisedExpression, terminal, activeLogin);
        }
    }

    public GatewaySessionHandler preCreateGatewaySessionHandler(String userId, String idType, String authorisedExpression, String deviceToken, Integer terminal, Boolean activeLogin, String authorisedToken) {
        checkState();
        ValidateUtils.checkAllNotNull(userId, deviceToken, terminal);
        if (authorisedExpression == null)
            authorisedExpression = this.authorisedExpression;
        if (activeLogin == null)
            activeLogin = false;


        GatewaySessionHandler gatewaySessionHandler = userIdGatewaySessionHandlerMap.get(userId);
        if (gatewaySessionHandler != null) {
            reLogin(gatewaySessionHandler, authorisedExpression, deviceToken, terminal, activeLogin, authorisedToken);
        } else {
            Class<? extends GatewaySessionHandler> clazz = gatewaySessionAnnotationHandler.getGatewaySessionHandlerClass(idType);
            if(clazz == null)
                throw new CoreException(NetErrors.ID_TYPE_NOT_FOUND, "GatewaySessionHandler class not found for idType {}", idType);
            try {
                gatewaySessionHandler = clazz.getConstructor().newInstance();
                InstanceFactory.injectBean(gatewaySessionHandler);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                     NoSuchMethodException e) {
                throw new CoreException(NetErrors.GATEWAY_SESSION_HANDLER_CLASS_NEW_FAILED, "GatewaySessionHandler class {} new failed, {}", clazz, e.getMessage());
            }
//        handler.userChannel = userChannel
            gatewaySessionHandler.setUserChannel(new UserChannel().userId(userId).authorisedExpression(authorisedExpression).deviceToken(deviceToken).terminal(terminal).createTime(System.currentTimeMillis()));
            gatewaySessionHandler.setId(gatewaySessionHandler.getUserChannel().getUserId());

            if(authorisedToken != null)
                verifyAuthorisedToken(gatewaySessionHandler, authorisedToken);

            boolean exceedMaxChannels = true;
            if (roomCounter < this.maxChannels) {
                synchronized (userLock) {
                    if (roomCounter < this.maxChannels) {
                        GatewaySessionHandler existing = userIdGatewaySessionHandlerMap.putIfAbsent(gatewaySessionHandler.getId(), gatewaySessionHandler);
                        if (existing == null) {
                            tokenUserIdMap.putIfAbsent(gatewaySessionHandler.getToken(), gatewaySessionHandler.getId());
                            roomCounter++;

                            SingleThreadBlockingQueue<UserAction> queue = getUserActionQueue(gatewaySessionHandler.getId());
                            queue.offer(new UserAction().handler(gatewaySessionHandler).userId(gatewaySessionHandler.getId()).action(UserAction.ACTION_SESSION_CREATED));
                        } else {
                            gatewaySessionHandler = existing;
                        }
                        exceedMaxChannels = false;
                    }
                }
            }
            if (exceedMaxChannels)
                throw new CoreException(WSErrors.ERROR_EXCEED_USER_CHANNEL_CAPACITY, "Exceed user channel capacity {}, current {}", this.maxChannels, roomCounter);
        }
        gatewaySessionHandler.touch();
        return gatewaySessionHandler;
    }

    public void invokeOnUserThread(String userId, Runnable closure) {
        if (userId == null || closure == null) {
            TapLogger.error(TAG, "invokeOnRoomThread ignored, because of illegal arguments, roomId {} closure {}", userId, closure);
            return;
        }

        GatewaySessionHandler gatewaySessionHandler = userIdGatewaySessionHandlerMap.get(userId);
        if (gatewaySessionHandler == null) {
            TapLogger.error(TAG, "invokeOnRoomThread ignored, because room doesn't exist for roomId {} closure {}", userId, closure);
            return;
        }
        SingleThreadBlockingQueue<UserAction> queue = getUserActionQueue(gatewaySessionHandler.getId());
        queue.offer(new UserAction().handler(gatewaySessionHandler).userId(gatewaySessionHandler.getId()).action(UserAction.ACTION_USER_CLOSURE).closure(closure));
    }

    public void closeSession(String userId) {
        closeSession(userId, true);
    }

    public void closeSession(String userId, boolean closeQuietly) {
        checkState();

        GatewaySessionHandler gatewaySessionHandler;
//        gatewayManagerService.logout(userId, context.serverConfig.service);
        SingleThreadBlockingQueue<UserAction> queue;
        synchronized (userLock) {
            gatewaySessionHandler = userIdGatewaySessionHandlerMap.remove(userId);
            if (gatewaySessionHandler == null) {
                if (closeQuietly) {
                    return;
                }
                throw new CoreException(WSErrors.ERROR_GATEWAY_CHANNEL_NOT_EXIST, "Gateway channel {} not exist while closing gateway channel", userId);
            }
            queue = removeUserActionQueue(userId);
            tokenUserIdMap.remove(gatewaySessionHandler.getToken());
            roomCounter--;
        }
        gatewayChannelModule.close(gatewaySessionHandler.getId(), WSErrors.ERROR_CHANNEL_USER_CLOSED);

//        SingleThreadQueueEx<UserAction> queue = getUserActionQueue(gatewaySessionHandler.id)
        if(queue != null){
            queue.offer(new UserAction().handler(gatewaySessionHandler).userId(gatewaySessionHandler.getId()).action(UserAction.ACTION_SESSION_DESTROYED));
        }
    }

    public void closeChannel(String userId, int code) {
        closeChannel(userId, code, true);
    }

    public void closeChannel(String userId, int code, boolean closeQuietly) {
        GatewaySessionHandler gatewaySessionHandler = userIdGatewaySessionHandlerMap.get(userId);
        if (gatewaySessionHandler == null) {
            if (closeQuietly) {
                return;
            }
            throw new CoreException(WSErrors.ERROR_USER_NOT_EXIST, "Gateway user {} not exist while closing channel", userId);
        }

        if (gatewayChannelModule.close(userId, code)) {
            SingleThreadBlockingQueue<UserAction> queue = getUserActionQueue(userId);
            queue.offer(new UserAction().handler(gatewaySessionHandler).userId(userId).action(UserAction.ACTION_USER_DISCONNECTED));
        }
    }

    public void refreshHandlerToken(String userId) {
        GatewaySessionHandler gatewaySessionHandler = userIdGatewaySessionHandlerMap.get(userId);
        if (gatewaySessionHandler == null)
            throw new CoreException(WSErrors.ERROR_USER_NOT_EXIST, "User {} not exist while refreshHandlerToken", userId);

        synchronized (userLock) {
            String old = gatewaySessionHandler.getToken();
            gatewaySessionHandler.setToken(UUID.randomUUID().toString().replace("-", ""));
            tokenUserIdMap.remove(old);
            tokenUserIdMap.put(gatewaySessionHandler.getToken(), gatewaySessionHandler.getId());
        }

    }

    public GatewaySessionHandler getGatewaySessionHandler(String userId) {
        return userIdGatewaySessionHandlerMap.get(userId);
    }

    public void receiveIncomingData(String userId, IncomingData incomingData) {
        GatewaySessionHandler gatewaySessionHandler = userIdGatewaySessionHandlerMap.get(userId);
        if (gatewaySessionHandler == null)
            throw new CoreException(WSErrors.ERROR_USER_NOT_EXIST, "User {} not exist while receiving incomingData", userId);

        //接收消息采用房间的消息单线程来处理， 和房间状态单线程分开
        SingleThreadBlockingQueue<UserAction> queue = getUserActionQueue(userId/* + "_message" 暂时不考虑每个房间收消息是独立的单线程*/);
        queue.offer(new UserAction().handler(gatewaySessionHandler).incomingData(incomingData).userId(userId).action(UserAction.ACTION_USER_DATA));
    }

    public void receiveIncomingMessage(String userId, IncomingMessage incomingMessage) {
        GatewaySessionHandler gatewaySessionHandler = userIdGatewaySessionHandlerMap.get(userId);
        if (gatewaySessionHandler == null)
            throw new CoreException(WSErrors.ERROR_USER_NOT_EXIST, "User {} not exist while receiving incomingMessage", userId);

        //接收消息采用房间的消息单线程来处理， 和房间状态单线程分开
        SingleThreadBlockingQueue<UserAction> queue = getUserActionQueue(userId/* + "_message" 暂时不考虑每个房间收消息是独立的单线程*/);
        queue.offer(new UserAction().handler(gatewaySessionHandler).incomingMessage(incomingMessage).userId(userId).action(UserAction.ACTION_USER_MESSAGE));
    }

    public void receiveIncomingInvocation(String userId, IncomingInvocation incomingInvocation) {
        GatewaySessionHandler gatewaySessionHandler = userIdGatewaySessionHandlerMap.get(userId);
        if (gatewaySessionHandler == null)
            throw new CoreException(WSErrors.ERROR_USER_NOT_EXIST, "User {} not exist while receiving incomingInvocation", userId);

        //接收消息采用房间的消息单线程来处理， 和房间状态单线程分开
        SingleThreadBlockingQueue<UserAction> queue = getUserActionQueue(userId/* + "_message" 暂时不考虑每个房间收消息是独立的单线程*/);
        queue.offer(new UserAction().handler(gatewaySessionHandler).incomingInvocation(incomingInvocation).userId(userId).action(UserAction.ACTION_USER_INVOCATION));
    }

    public void receiveIncomingRequest(String userId, IncomingRequest incomingRequest) {
        GatewaySessionHandler gatewaySessionHandler = userIdGatewaySessionHandlerMap.get(userId);
        if (gatewaySessionHandler == null)
            throw new CoreException(WSErrors.ERROR_USER_NOT_EXIST, "User {} not exist while receiving incomingInvocation", userId);

        //接收消息采用房间的消息单线程来处理， 和房间状态单线程分开
        SingleThreadBlockingQueue<UserAction> queue = getUserActionQueue(userId/* + "_message" 暂时不考虑每个房间收消息是独立的单线程*/);
        queue.offer(new UserAction().handler(gatewaySessionHandler).incomingRequest(incomingRequest).userId(userId).action(UserAction.ACTION_USER_REQUEST));
    }

    public void checkState() {
        if (stateMachine.getCurrentState() != STATE_STARTED)
            throw new CoreException(WSErrors.ERROR_GATEWAY_SESSION_MANAGER_NOT_STARTED, "GatewaySessionManager not started");
        if (gatewaySessionAnnotationHandler.isEmpty())
            throw new CoreException(WSErrors.ERROR_GATEWAY_SESSION_HANDLER_CLASS_IS_NULL, "gatewaySessionHandlerClass is not found");
    }

    public int roomCounter() {
        return roomCounter;
    }
    /**
     * 找到玩家，单线程发消息
     * @param userId
     * @param outgoingMessage
     */
    public void receiveOutgoingMessage(String userId, OutgoingMessage outgoingMessage) {
        GatewaySessionHandler gatewaySessionHandler = userIdGatewaySessionHandlerMap.get(userId);
        if (gatewaySessionHandler == null)
            throw new CoreException(WSErrors.ERROR_USER_NOT_EXIST, "User {} not exist while receiving outgoingMessage", userId);

        //接收消息采用房间的消息单线程来处理， 和房间状态单线程分开
        SingleThreadBlockingQueue<UserAction> queue = getUserActionQueue(userId/* + "_message" 暂时不考虑每个房间收消息是独立的单线程*/);
        queue.offer(new UserAction().handler(gatewaySessionHandler).outgoingMessage(outgoingMessage).userId(userId).action(UserAction.ACTION_USER_OUTGOING_MESSAGE));
    }

    public void receiveOutgoingData(String userId, OutgoingData outgoingData) {
        GatewaySessionHandler gatewaySessionHandler = userIdGatewaySessionHandlerMap.get(userId);
        if (gatewaySessionHandler == null)
            throw new CoreException(WSErrors.ERROR_USER_NOT_EXIST, "User {} not exist while receiving outgoingData", userId);

        //接收消息采用房间的消息单线程来处理， 和房间状态单线程分开
        SingleThreadBlockingQueue<UserAction> queue = getUserActionQueue(userId/* + "_message" 暂时不考虑每个房间收消息是独立的单线程*/);
        queue.offer(new UserAction().handler(gatewaySessionHandler).outgoingData(outgoingData).userId(userId).action(UserAction.ACTION_USER_OUTGOING_DATA));
    }

    /**
     * 缓存当InstantMessage#cacheTimeKey不为空，
     * 更新时间GatewaySessionHandler#cacheKeyToTimeMap
     * @param cacheTimeKey
     * @param time
     * @param userId
     */
    public void putCacheKeyToTime(String cacheTimeKey, Long time, String userId) {
        if (StringUtils.isNotEmpty(cacheTimeKey) && time != null) {
            GatewaySessionHandler gatewaySessionHandler = userIdGatewaySessionHandlerMap.get(userId);
            if (gatewaySessionHandler != null) {
                gatewaySessionHandler.getCacheKeyToTime().put(cacheTimeKey, time);
            }
        }
    }

    /**
     * 通过验证，正式连接
     * @param gatewaySessionHandler
     * @return
     */
    public TapEntity channelConnected(GatewaySessionHandler gatewaySessionHandler) {
        try {
            //异步消息
            SingleThreadBlockingQueue<UserAction> queue = getUserActionQueue(gatewaySessionHandler.getId());
            queue.offer(new UserAction().handler(gatewaySessionHandler).userId(gatewaySessionHandler.getId()).action(UserAction.ACTION_USER_CONNECTED));
            //同步消息
//            return map(entry("cacheTimeMap", gatewaySessionHandler.channelConnected()));
            return null;
        } catch (Throwable t) {
            TapLogger.error(TAG, "channelConnected userId:{} error:{}", gatewaySessionHandler.getId(), t);
        }
        return null;
    }

    public void channelDisconnected(GatewaySessionHandler gatewaySessionHandler) {
        try {
            SingleThreadBlockingQueue<UserAction> queue = getUserActionQueue(gatewaySessionHandler.getId());
            queue.offer(new UserAction().handler(gatewaySessionHandler).userId(gatewaySessionHandler.getId()).action(UserAction.ACTION_USER_DISCONNECTED));
        } catch (Throwable t) {
            TapLogger.error(TAG, "channelDisconnected userId:{} error:{}", gatewaySessionHandler.getId(), t);
        }
    }

    public OutgoingMessageFilter getBroadcastOutgoingMessageFilter() {
        return broadcastOutgoingMessageFilter;
    }

    public void setBroadcastOutgoingMessageFilter(OutgoingMessageFilter broadcastOutgoingMessageFilter) {
        this.broadcastOutgoingMessageFilter = broadcastOutgoingMessageFilter;
    }

    public Map<String, String> getTokenUserIdMap() {
        return tokenUserIdMap;
    }

    public ConcurrentHashMap<String, GatewaySessionHandler> getUserIdGatewaySessionHandlerMap() {
        return userIdGatewaySessionHandlerMap;
    }

    @Override
    public int weight() {
        return 100;
    }

    @Override
    public int health() {
        return roomCounter * 10000 / maxChannels;
    }

    public NodeRegistry getNodeRegistry() {
        return nodeRegistry;
    }

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        DataMap dataMap = DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/
                .kv("threadPoolExecutor", threadPoolExecutor.toString())
                .kv("maxChannels", maxChannels)
                .kv("nodeHealthManager", nodeHealthManager.memory(keyRegex, memoryLevel))
                .kv("gatewayChannelModule", this.gatewayChannelModule.memory(keyRegex, memoryLevel))
                ;
        DataMap userIdGatewaySessionHandlerMap = DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/;
        dataMap.kv("userIdGatewaySessionHandlerMap", userIdGatewaySessionHandlerMap);
        for(Map.Entry<String, GatewaySessionHandler> entry : this.userIdGatewaySessionHandlerMap.entrySet()) {
            userIdGatewaySessionHandlerMap.kv(entry.getKey(), entry.getValue().memory(keyRegex, memoryLevel));
        }

        DataMap userIdSingleThreadMap = DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/;
        dataMap.kv("userIdSingleThreadMap", userIdSingleThreadMap);
        for(Map.Entry<String, SingleThreadBlockingQueue<UserAction>> entry : this.userIdSingleThreadMap.entrySet()) {
            userIdSingleThreadMap.put(entry.getKey(), entry.getValue().memory(keyRegex, memoryLevel));
        }

        return dataMap;
    }
}
