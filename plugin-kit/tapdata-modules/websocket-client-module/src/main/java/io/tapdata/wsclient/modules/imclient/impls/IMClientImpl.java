package io.tapdata.wsclient.modules.imclient.impls;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.modules.api.net.data.Data;
import io.tapdata.modules.api.net.data.IncomingData;
import io.tapdata.modules.api.net.data.IncomingMessage;
import io.tapdata.modules.api.net.data.Result;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.wsclient.modules.imclient.IMClient;
import io.tapdata.wsclient.modules.imclient.impls.websocket.WebsocketPushChannel;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class IMClientImpl implements IMClient {
    private static final String TAG = IMClient.class.getSimpleName();
    private String clientId;
    private String service;
    private List<String> baseUrls;
    private String token;
    private String cachedAccessToken;
    private String cachedCookie;
    private Integer terminal;

    private AtomicLong msgCounter;

    private String prefix;


    LinkedBlockingQueue<Data> messageQueue;
//    WorkerQueue<IMData> messageWorkerQueue;
    ConcurrentHashMap<String, ResultListenerWrapper> resultMap;
    ConcurrentHashMap<String, Class<? extends Data>> contentTypeClassMap;

    private MonitorThread<?> monitorThread;
//    static {
//        System.setProperty
//                ("java.util.concurrent.ForkJoinPool.common.parallelism", "4");
//        System.setProperty
//                ("java.util.concurrent.ForkJoinPool.common.threadFactory", "io.tapdata.wsclient.modules.imclient.impls.IMClientThreadFactory");
//        System.setProperty
//                ("java.util.concurrent.ForkJoinPool.common.exceptionHandler", "io.tapdata.wsclient.modules.imclient.impls.IMClientUncaughtExceptionHandler");
//    }


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

    public CompletableFuture<Result> sendData(IncomingData data) {
        return sendData(data, null);
    }

    @Override
    public CompletableFuture<Result> sendData(IncomingData data, Integer expireSeconds) {
        if(expireSeconds == null)
            expireSeconds = 60;
        CompletableFuture<Result> future = new CompletableFuture<>();
        String msgId = data.getId();
        long counter = msgCounter.getAndIncrement();
        if(counter < 0)
            TapLogger.error(TAG, "That's amazing that you have use out all the long value... Need reset the using values, but not implemented yet.");
        if(msgId == null) {
            msgId = "DATA_" + counter;
            data.setId(msgId);
        }
        ScheduledFuture scheduledFuture = getTimeoutScheduledFuture(data, future, expireSeconds);
        resultMap.put(msgId, new ResultListenerWrapper(data, future, scheduledFuture, counter));
        sendDataInternal(data);
        return future;
    }

    void sendDataInternal(IncomingData data) {
        try {
            if(messageQueue.offer(data, 60, TimeUnit.SECONDS)) {
                monitorThread.wakeupForMessage();
            } else {
                sendFailed(data.getId(), new CoreException(NetErrors.QUEUE_IS_FULL, "WS Client send queue is full"));
            }
        } catch (InterruptedException e) {
            sendFailed(data.getId(), e);
        }
    }

    private void sendFailed(String msgId, Throwable e) {
        ResultListenerWrapper wrapper = resultMap.get(msgId);
        if(wrapper != null)
            wrapper.completeExceptionally(resultMap, e);
    }

    public CompletableFuture<Result> sendMessage(IncomingMessage message) {
        return sendMessage(message, null);
    }
    @Override
    public CompletableFuture<Result> sendMessage(IncomingMessage message, Integer expireSeconds) {
        if(expireSeconds == null)
            expireSeconds = 600;
        CompletableFuture<Result> future = new CompletableFuture<>();
        String msgId = message.getId();
        long counter = msgCounter.getAndIncrement();
        if(counter < 0)
            TapLogger.error(TAG, "That's amazing that you have use out all the long value... Need reset the using values, but not implemented yet.");
        if(msgId == null) {
            msgId = "MSG_" + counter;
            message.setId(msgId);
        }
        ScheduledFuture scheduledFuture = getTimeoutScheduledFuture(message, future, expireSeconds);
        resultMap.put(msgId, new ResultListenerWrapper(message, future, scheduledFuture, counter));
        messageQueue.offer(message);
        monitorThread.wakeupForMessage();
        return future;
    }

    private ScheduledFuture getTimeoutScheduledFuture(Data message, CompletableFuture<Result> future, Integer expireSeconds) {
        return ExecutorsManager.getInstance().getScheduledExecutorService().schedule(() -> {
            synchronized (message) {
                if(!future.isDone())
                    future.completeExceptionally(new IOException("Connect to Manager time out after " + expireSeconds + " seconds"));
                resultMap.remove(message.getId());
            }
        }, expireSeconds, TimeUnit.SECONDS);
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

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        DataMap resultMap = DataMap.create().keyRegex(keyRegex);
        DataMap memory =  DataMap.create().keyRegex(keyRegex)
                .kv("clientId", clientId)
                .kv("baseUrls", baseUrls)
                .kv("messageQueue", messageQueue.toString())
                .kv("resultMap", resultMap)
                .kv("monitorThread", monitorThread.memory(keyRegex, memoryLevel))
                ;
        for(Map.Entry<String, ResultListenerWrapper> entry : this.resultMap.entrySet()) {
            resultMap.kv(entry.getKey(), entry.getValue().memory(keyRegex, memoryLevel));
        }
        return memory;
    }

    public String getCachedAccessToken() {
        return cachedAccessToken;
    }

    public void setCachedAccessToken(String cachedAccessToken) {
        this.cachedAccessToken = cachedAccessToken;
    }

    public String getCachedCookie() {
        return cachedCookie;
    }

    public void setCachedCookie(String cachedCookie) {
        this.cachedCookie = cachedCookie;
    }
}
