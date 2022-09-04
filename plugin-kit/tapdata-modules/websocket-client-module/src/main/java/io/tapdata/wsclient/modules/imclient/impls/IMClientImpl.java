package io.tapdata.wsclient.modules.imclient.impls;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.modules.api.net.data.Data;
import io.tapdata.modules.api.net.data.IncomingData;
import io.tapdata.modules.api.net.data.IncomingMessage;
import io.tapdata.modules.api.net.data.Result;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.wsclient.modules.imclient.IMClient;
import io.tapdata.wsclient.modules.imclient.impls.websocket.WebsocketPushChannel;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class IMClientImpl implements IMClient {
    private static final String TAG = IMClient.class.getSimpleName();
    private String clientId;
    private String service;
    private List<String> baseUrls;
    private String token;
    private Integer terminal;

    private AtomicLong msgCounter;

    private String prefix;

    LinkedBlockingQueue<Data> messageQueue;
//    WorkerQueue<IMData> messageWorkerQueue;
    ConcurrentHashMap<String, ResultListenerWrapper> resultMap;
    ConcurrentHashMap<String, Class<? extends Data>> contentTypeClassMap;

    private MonitorThread<?> monitorThread;
    static {
        System.setProperty
                ("java.util.concurrent.ForkJoinPool.common.parallelism", "4");
        System.setProperty
                ("java.util.concurrent.ForkJoinPool.common.threadFactory", "com.dobybros.tccore.modules.imclient.impls.IMClientThreadFactory");
        System.setProperty
                ("java.util.concurrent.ForkJoinPool.common.exceptionHandler", "com.dobybros.tccore.modules.imclient.impls.IMClientUncaughtExceptionHandler");
    }


    public IMClientImpl(String prefix, String clientId, String service, Integer terminal, String token, List<String> baseUrls) {
        this.prefix = prefix;
        this.clientId = clientId;
        this.service = service;
        this.baseUrls = baseUrls;
        this.token = token;
        this.terminal = terminal;

        msgCounter = new AtomicLong(0);
        resultMap = new ConcurrentHashMap<>();
        contentTypeClassMap = new ConcurrentHashMap<>();
        messageQueue = new LinkedBlockingQueue<>(1024);

//        messageWorkerQueue = new WorkerQueue<>();
//        messageWorkerQueue.setHandler(new WorkerQueue.Handler<IMMessage>() {
//            @Override
//            public boolean handle(IMMessage message) {
//                return true;
//            }
//        });
    }

    @Override
    public void start() {
        stop();
        TapLogger.info(TAG, "IMClient started");
        monitorThread = new MonitorThread<>(WebsocketPushChannel.class/*TcpPushChannel.class*/);
        monitorThread.setImClient(this);
//        messageWorkerQueue.setHandler(monitorThread.new PushHandler());
        monitorThread.start();
    }

    @Override
    public void stop() {
        if(monitorThread != null) {
            monitorThread.terminate();
            monitorThread = null;
        }
    }

    @Override
    public CompletableFuture<Result> sendData(IncomingData data) {
        CompletableFuture<Result> future = new CompletableFuture<>();
        String msgId = data.getId();
        if(msgId == null) {
            msgId = "DATA_" + msgCounter.getAndIncrement();
            data.setId(msgId);
        }
        ScheduledFuture scheduledFuture = getTimeoutScheduledFuture(data, future);
        resultMap.put(msgId, new ResultListenerWrapper(msgId, future, scheduledFuture));
        messageQueue.offer(data);
        monitorThread.wakeupForMessage();
        return future;
    }

    @Override
    public CompletableFuture<Result> sendMessage(IncomingMessage message) {
        CompletableFuture<Result> future = new CompletableFuture<>();
        String msgId = message.getId();
        if(msgId == null) {
            msgId = "MSG_" + msgCounter.getAndIncrement();
            message.setId(msgId);
        }
        ScheduledFuture scheduledFuture = getTimeoutScheduledFuture(message, future);
        resultMap.put(msgId, new ResultListenerWrapper(msgId, future, scheduledFuture));
        messageQueue.offer(message);
        monitorThread.wakeupForMessage();
        return future;
    }

    private ScheduledFuture getTimeoutScheduledFuture(Data message, CompletableFuture<Result> future) {
        return ExecutorsManager.getInstance().getScheduledExecutorService().schedule(() -> {
            if(!future.isDone())
                future.completeExceptionally(new IOException("Time out"));
            resultMap.remove(message.getId());
        }, 30, TimeUnit.SECONDS);
    }

    @Override
    public void registerDataContentType(String contentType, Class<? extends Data> dataClass) {
        if(contentType == null || dataClass == null) return;
        contentTypeClassMap.put(contentType, dataClass);
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public List<String> getBaseUrls() {
        return baseUrls;
    }

    public void setBaseUrls(List<String> baseUrls) {
        this.baseUrls = baseUrls;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public Integer getTerminal() {
        return terminal;
    }

    public void setTerminal(Integer terminal) {
        this.terminal = terminal;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public LinkedBlockingQueue<Data> getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(LinkedBlockingQueue<Data> messageQueue) {
        this.messageQueue = messageQueue;
    }

    public ConcurrentHashMap<String, ResultListenerWrapper> getResultMap() {
        return resultMap;
    }

    public void setResultMap(ConcurrentHashMap<String, ResultListenerWrapper> resultMap) {
        this.resultMap = resultMap;
    }

    public ConcurrentHashMap<String, Class<? extends Data>> getContentTypeClassMap() {
        return contentTypeClassMap;
    }

    public void setContentTypeClassMap(ConcurrentHashMap<String, Class<? extends Data>> contentTypeClassMap) {
        this.contentTypeClassMap = contentTypeClassMap;
    }
}
