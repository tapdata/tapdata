package io.tapdata.wsclient.modules.imclient.impls;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.memory.LastData;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.modules.api.net.data.Data;
import io.tapdata.modules.api.net.data.IncomingData;
import io.tapdata.modules.api.net.data.IncomingMessage;
import io.tapdata.modules.api.net.data.Result;
import io.tapdata.wsclient.modules.imclient.impls.websocket.ChannelStatus;
import io.tapdata.wsclient.utils.EventManager;

import io.tapdata.entity.error.CoreException;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;

public class MonitorThread<T extends PushChannel> extends Thread implements MemoryFetcher {
    public static final int CHANNEL_ERRORS_LOGIN_FAILED = 110;

    public static final int STATUS_IDLE = 1;
    public static final int STATUS_STARTED = 2;
    public static final int STATUS_TERMINATED = 4;
    private static final String TAG = MonitorThread.class.getSimpleName();

    private String lastBaseUrl;
    private int baseUrlIndex = 0;
    private int baseUrlRetryCount = 0;
    public int getStatus() {
        return status;
    }

    private int status = STATUS_IDLE;

    private T pushChannel;
    private final int[] lock = new int[0];
    private final int[] channelLock = new int[0];

    private int count = 0;
    private final long[] idleTimes = new long[]{RETRY_TIME, 1000, 2000, 3000, 4000, 5000, 10000};
    private long idleTime = 0;
    private static final long RETRY_TIME = 300;

    private Class<T> pushChannelClass;

    private IMClientImpl imClient;
    private EventManager eventManager;

    private LastData lastConnected, lastConnectError, lastDataError;

    public String status() {
        switch (status) {
            case STATUS_IDLE:
                return "Idle";
            case STATUS_STARTED:
                return "Started";
            case STATUS_TERMINATED:
                return "Terminated";
            default:
                return "Unknown " + status;
        }
    }
    public MonitorThread(Class<T> pushChannelClass) {
        super("MonitorThread");
        this.pushChannelClass = pushChannelClass;
        eventManager = EventManager.getInstance();
    }

    @Override
    public synchronized final void start() {
        if(status == STATUS_STARTED) {
            if(pushChannel == null) {
                synchronized (lock) {
                    idleTime = 0;
                    count = 0;
                    lock.notifyAll();
                }
            } else {
                pushChannel.selfCheck();
            }
            TapLogger.info(TAG, "MonitorThread already started, notify or self check");
        } else if(status == STATUS_IDLE) {
            status = STATUS_STARTED;
            if(!this.isAlive()) {
                super.start();
                TapLogger.info(TAG, "MonitorThread started");
            }
        }
    }

    public synchronized final void terminate() {
        if(status == STATUS_TERMINATED)
            return;
        TapLogger.info(TAG, "MonitorThread terminated");

        status = STATUS_TERMINATED;
        if(pushChannel != null)
            pushChannel.stop();
        synchronized (lock) {
            idleTime = 0;
            count = 0;
            lock.notifyAll();
        }
        eventManager.unregisterEventListener(this);
    }

    public void wakeupForMessage() {
        if(pushChannel != null) {
            synchronized (lock) {
                lock.notifyAll();
            }
        }
    }

    public void restartChannel() {
        restartChannel(true);
    }

    public void restartChannel(boolean hurry) {
        TapLogger.info(TAG, "MonitorThread restart channel, " + (hurry ? "" : "no ") + "hurry");
        if(pushChannel != null) {
            pushChannel.stop();
            synchronized (channelLock) {
                pushChannel = null;
            }
        }
        final int MAX = 5;
        if(count > MAX) {
            //Switch to another baseUrls to reconnect.
            List<String> baseUrls = imClient.getBaseUrls();
            int baseUrlSize = baseUrls.size();
            if(baseUrlSize > 1) {
                hurry = true;
                if(baseUrlIndex + 1 >= baseUrlSize) {
                    baseUrlIndex = 0;
                } else {
                    baseUrlIndex++;
                }
                String old = lastBaseUrl;
                lastBaseUrl = baseUrls.get(baseUrlIndex);
                TapLogger.info(TAG, "Will reconnect other url {} as already retry {} times on url {}", lastBaseUrl, MAX, old);
            }
        }
        synchronized (lock) {
            if(hurry) {
                resetIdleTimes();
            }
            lock.notifyAll();
        }
    }

    private void shiftIdleTimes() {
        if(count < Integer.MAX_VALUE)
            count++;
        if(count > idleTimes.length) {
            idleTime = idleTimes[idleTimes.length - 1];
        } else {
            idleTime = idleTimes[count - 1];
        }
    }

    private void resetIdleTimes() {
        count = 0;
        idleTime = RETRY_TIME;
    }

    void handleMessageSendFailed(Data imData, String message) {
        if(imData == null) return ;
        TapLogger.info(TAG, "send message failed, " + message + " imdata " + imData);
        ResultListenerWrapper wrapper = imClient.resultMap.get(imData.getId());
        if(wrapper != null) {
            wrapper.completeExceptionally(imClient.resultMap, new IOException(message));
//            wrapper.getResultHandler().completeExceptionally(new IOException(message));
//            imClient.resultMap.remove(imData.getId());
        }
    }

    void sendMessageFromQueue() {
        if(!imClient.messageQueue.isEmpty() && pushChannel != null) {
            Data imData = null;
            while((imData = imClient.messageQueue.poll()) != null) {
                try {
                    if(!MonitorThread.this.send(imData)) {
                        handleMessageSendFailed(imData, "Send message after connected failed because channel just disconnected.");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    handleMessageSendFailed(imData, "Send message after connected failed, " + e.getMessage());
                }
            }
        }
    }

    boolean inteceptErrorResult(Result result) {
        if(result == null) return false;
        Integer code = result.getCode();
        if(code == null) return false;

        return false;
    }

    public void run() {
        TapLogger.info(TAG, "Monitor Thread is running");
        eventManager.registerEventListener(this, imClient.getPrefix() + ".status", (EventManager.EventListener<ChannelStatus>) (eventType, channelStatus) -> {
            TapLogger.info(TAG, "status changed, " + channelStatus);
            switch (channelStatus.getStatus()) {
                case ChannelStatus.STATUS_CONNECTED:
                    lastConnected = new LastData().data("Connected").time(System.currentTimeMillis());
                    sendMessageInWaitingResultState();
                    resetIdleTimes();
                    wakeupForMessage();
                    break;
                case ChannelStatus.STATUS_DISCONNECTED:
                    if(pushChannel != null && channelStatus.getPushChannel().equals(pushChannel)) {
                        lastConnectError = new LastData().error("code " + channelStatus.getCode() + " reason " + channelStatus.getReason()).time(System.currentTimeMillis());
                        failedAllPendingMessages();
                        shiftIdleTimes();
                        restartChannel(false);
                    }
                    break;
                case ChannelStatus.STATUS_OFFLINEMESSAGECONSUMED:
                    break;
                case ChannelStatus.STATUS_BYE:
                case ChannelStatus.STATUS_KICKED:
                    lastConnected = new LastData().error("Kicked or Bye code " + channelStatus.getCode() + " reason " + channelStatus.getReason()).time(System.currentTimeMillis());
                    failedAllPendingMessages();
                    resetIdleTimes();
                    if(pushChannel != null && channelStatus.getPushChannel().equals(pushChannel)) {
                        terminate();
                    }
                    break;
            }
        });
        eventManager.registerEventListener(this, imClient.getPrefix() + ".data", (EventManager.EventListener<Data>) (eventType, message) -> {
            if(message != null && message.getContentType() != null) {
                Class<? extends Data> dataClass = imClient.contentTypeClassMap.get(message.getContentType());
                if(dataClass != null) {
                    TapLogger.debug(TAG, "PushChannel receive imdata " + message);
                    eventManager.sendEvent(imClient.getPrefix() + ".imdata", message);
                    eventManager.sendEvent(imClient.getPrefix() + ".imdata." + message.getContentType(), message);
                }
            }
        });
        eventManager.registerEventListener(this, imClient.getPrefix() + ".result", (EventManager.EventListener<Result>) (eventType, result) -> {
//            if(inteceptErrorResult(result)) {
//               return;
//            }
            String id = result.getForId();
            if(id != null) {
                ResultListenerWrapper wrapper = imClient.resultMap.get(id);
                if(wrapper != null) {
                    if(result.getCode() == 1) {
                        wrapper.complete(imClient.resultMap, result);
                    } else {
                        lastDataError = new LastData().error("Result id " + result.getForId() + " code " + result.getCode() + " description " + result.getDescription()).time(System.currentTimeMillis());
                        wrapper.completeExceptionally(imClient.resultMap, new CoreException(result.getCode(), result.getDescription()));
                    }
                } else {
                    eventManager.sendEvent(imClient.getPrefix() + ".imresult", result);
                }
            }
        });
        while(status == STATUS_STARTED) {
            try {
                if(pushChannel == null) {
                    synchronized (lock) {
                        try {
                            if(idleTime > 0)
                                lock.wait(idleTime);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    if(status == STATUS_STARTED) {
                        if(pushChannelClass != null) {
                            Constructor<? extends PushChannel> constructor = pushChannelClass.getConstructor();
                            pushChannel = (T)constructor.newInstance();
                            pushChannel.setImClient(imClient);
                            if(lastBaseUrl == null) {
                                lastBaseUrl = imClient.getBaseUrls().get(baseUrlIndex);
                                //TODO need to choose other base urls to reconnect.
                            }
                            pushChannel.start(lastBaseUrl);
                        }
                    }
                } else {
                    sendMessageFromQueue();
                    long time = System.currentTimeMillis();
                    synchronized (lock) {
                        try {
                            lock.wait(7000L);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    if(System.currentTimeMillis() - time > 6000L) {
                        //When message is sending fast, no need to ping. wait at least 6 seconds, then ping.
                        synchronized (channelLock) {
                            if(pushChannel != null) {
                                pushChannel.ping();
                            }
                        }
                    }
//                    TapLogger.debug( "PushControl thread wake up for creating new channel " + pushChannel);
                }
            } catch (Exception e) {
                e.printStackTrace();
                TapLogger.error(TAG, "error occurred {}", e.getMessage());
            }
        }
//		try {
//		} finally {
//			status = STATUS_TERMINATED;
//		}
    }

    private void sendMessageInWaitingResultState() {
        Data imData;
        List<Data> pendingDataList = null;
        if(!imClient.messageQueue.isEmpty()) {
            pendingDataList = new ArrayList<>();
            while((imData = imClient.messageQueue.poll()) != null) {
                pendingDataList.add(imData);
            }
        }

        TreeSet<ResultListenerWrapper> set = new TreeSet<>(imClient.resultMap.values());
        for(ResultListenerWrapper wrapper : set) {
            if(wrapper.getData() instanceof IncomingData) {
                IncomingData incomingData = (IncomingData) wrapper.getData();
                synchronized (incomingData) {
                    if(imClient.resultMap.containsKey(incomingData.getId())) {
                        imClient.sendDataInternal(incomingData);
                        TapLogger.debug(TAG, "Resend data {} after reconnect", incomingData);
                    }
                }
            } else if(wrapper.getData() instanceof IncomingMessage) {
                IncomingMessage incomingMessage = (IncomingMessage) wrapper.getData();
                synchronized (incomingMessage) {
                    if(imClient.resultMap.containsKey(incomingMessage.getId())) {
                        imClient.sendMessage(incomingMessage);
                        TapLogger.debug(TAG, "Resend message {} after reconnect", incomingMessage);
                    }
                }
            }
        }
        //keep in order.
        if(pendingDataList != null) {
            for(Data data : pendingDataList) {
                if(data instanceof IncomingData)
                    imClient.sendData((IncomingData) data);
                else if(data instanceof IncomingMessage)
                    imClient.sendMessage((IncomingMessage) data);
            }
        }
    }

    private void failedAllPendingMessages() {
        //Need retry to send
//        SortedSet<ResultListenerWrapper> list = new TreeSet<>();
//        Data imData;
//        while((imData = imClient.messageQueue.poll()) != null) {
//            handleMessageSendFailed(imData, "Send message because channel just disconnected.");
//        }
//        for(ResultListenerWrapper wrapper : imClient.resultMap.values()) {
//            wrapper.completeExceptionally(imClient.resultMap, new IOException("Send message because channel just disconnected."));
//        }
    }

    boolean send(Data message) throws IOException {
        synchronized (channelLock) {
            if(pushChannel != null) {
                TapLogger.debug(TAG, "send IMData " + message);
                pushChannel.send(message);
            } else {
                TapLogger.warn(TAG, "send IMData failed, channel is not connected, " + message);
                return false;
            }
            return true;
        }
    }

    public String toString() {
        return this.getName() + "|status:" + status + "|count:" + count;
    }

    public T getPushChannel() {
        return pushChannel;
    }

    public IMClientImpl getImClient() {
        return imClient;
    }

    public void setImClient(IMClientImpl imClient) {
        this.imClient = imClient;
    }

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        return DataMap.create().keyRegex(keyRegex)
                .kv("status", status())
                .kv("count", count)
                .kv("idleTime", idleTime)
                .kv("pushChannel", pushChannel != null ? pushChannel.memory(keyRegex, memoryLevel) : "null")
                .kv("lastBaseUrl", lastBaseUrl)
                .kv("baseUrlIndex", baseUrlIndex)
                .kv("lastConnected", lastConnected)
                .kv("lastConnectError", lastConnectError)
                .kv("lastDataError", lastDataError)
                ;
    }
}
