package com.dobybros.tccore.modules.imclient.impls;

import com.dobybros.tccore.modules.imclient.IMClient;
import com.dobybros.tccore.modules.imclient.data.IMData;
import com.dobybros.tccore.modules.imclient.data.IMMessage;
import com.dobybros.tccore.modules.imclient.data.IMResult;
import com.dobybros.tccore.modules.imclient.impls.websocket.WebsocketPushChannel;
import com.dobybros.tccore.utils.LoggerEx;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class IMClientImpl implements IMClient {
    private static final String TAG = IMClient.class.getSimpleName();
    private String userId;
    private String service;
    private String loginUrl;
    private String token;
    private Integer terminal;

    private AtomicLong msgCounter;

    private String prefix;

    LinkedBlockingQueue<IMData> messageQueue;
//    WorkerQueue<IMData> messageWorkerQueue;
    ConcurrentHashMap<String, ResultListenerWrapper> resultMap;
    ConcurrentHashMap<String, Class<? extends IMData>> contentTypeClassMap;

    private MonitorThread monitorThread;

    public IMClientImpl(String prefix, String userId, String service, Integer terminal, String token, String loginUrl) {
        this.prefix = prefix;
        this.userId = userId;
        this.service = service;
        this.loginUrl = loginUrl;
        this.token = token;
        this.terminal = terminal;

        msgCounter = new AtomicLong(0);
        resultMap = new ConcurrentHashMap<>();
        contentTypeClassMap = new ConcurrentHashMap<>();
        messageQueue = new LinkedBlockingQueue<>();

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
        LoggerEx.info(TAG, "IMClient started");
        monitorThread = new MonitorThread(WebsocketPushChannel.class/*TcpPushChannel.class*/);
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
    public CompletableFuture<IMResult> sendData(IMData data) {
        CompletableFuture<IMResult> future = new CompletableFuture<>();
        String msgId = data.getId();
        if(msgId == null) {
            msgId = "DATA_" + msgCounter.getAndIncrement();
            data.setId(msgId);
        }
        resultMap.put(msgId, new ResultListenerWrapper(msgId, future));
        messageQueue.offer(data);
        monitorThread.wakeupForMessage();
        return future;
    }

    @Override
    public CompletableFuture<IMResult> sendMessage(IMMessage message) {
        CompletableFuture<IMResult> future = new CompletableFuture<>();
        String msgId = message.getId();
        if(msgId == null) {
            msgId = "MSG_" + msgCounter.getAndIncrement();
            message.setId(msgId);
        }
        resultMap.put(msgId, new ResultListenerWrapper(msgId, future));
        messageQueue.offer(message);
        monitorThread.wakeupForMessage();
        return future;
    }

    @Override
    public void registerDataContentType(String contentType, Class<? extends IMData> dataClass) {
        if(contentType == null || dataClass == null) return;
        contentTypeClassMap.put(contentType, dataClass);
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getLoginUrl() {
        return loginUrl;
    }

    public void setLoginUrl(String loginUrl) {
        this.loginUrl = loginUrl;
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

    public LinkedBlockingQueue<IMData> getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(LinkedBlockingQueue<IMData> messageQueue) {
        this.messageQueue = messageQueue;
    }

    public ConcurrentHashMap<String, ResultListenerWrapper> getResultMap() {
        return resultMap;
    }

    public void setResultMap(ConcurrentHashMap<String, ResultListenerWrapper> resultMap) {
        this.resultMap = resultMap;
    }

    public ConcurrentHashMap<String, Class<? extends IMData>> getContentTypeClassMap() {
        return contentTypeClassMap;
    }

    public void setContentTypeClassMap(ConcurrentHashMap<String, Class<? extends IMData>> contentTypeClassMap) {
        this.contentTypeClassMap = contentTypeClassMap;
    }
}
