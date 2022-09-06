package io.tapdata.wsclient.modules.imclient.impls;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.modules.api.net.data.Data;
import io.tapdata.modules.api.net.data.Result;
import io.tapdata.wsclient.modules.imclient.impls.websocket.ChannelStatus;
import io.tapdata.wsclient.utils.EventManager;

import io.tapdata.entity.error.CoreException;

import java.io.IOException;
import java.lang.reflect.Constructor;

public class MonitorThread<T extends PushChannel> extends Thread {
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
    private final long[] idleTimes = new long[]{RETRY_TIME, 1000, 2000, 300, 4000, 5000, 10000};
    private long idleTime = 0;
    private static final long RETRY_TIME = 300;

    private Class<T> pushChannelClass;

    private IMClientImpl imClient;
    private EventManager eventManager;

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
        synchronized (lock) {
            if(hurry) {
                resetIdelTimes();
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

    private void resetIdelTimes() {
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
                    resetIdelTimes();
                    wakeupForMessage();
                    break;
                case ChannelStatus.STATUS_DISCONNECTED:
                    if(pushChannel != null && channelStatus.getPushChannel().equals(pushChannel)) {
                        failedAllPendingMessages();
                        shiftIdleTimes();
                        restartChannel(false);
                    }
                    break;
                case ChannelStatus.STATUS_OFFLINEMESSAGECONSUMED:
                    break;
                case ChannelStatus.STATUS_BYE:
                case ChannelStatus.STATUS_KICKED:
                    failedAllPendingMessages();
                    resetIdelTimes();
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
                    TapLogger.info(TAG, "PushChannel receive imdata " + message);
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
//                    TapLogger.info( "PushControl thread wake up for creating new channel " + pushChannel);
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

    private void failedAllPendingMessages() {
        Data imData;
        while((imData = imClient.messageQueue.poll()) != null) {
            handleMessageSendFailed(imData, "Send message because channel just disconnected.");
        }
        for(ResultListenerWrapper wrapper : imClient.resultMap.values()) {
            wrapper.completeExceptionally(imClient.resultMap, new IOException("Send message because channel just disconnected."));
        }
    }

    boolean send(Data message) throws IOException {
        synchronized (channelLock) {
            if(pushChannel != null) {
                TapLogger.info(TAG, "send IMData " + message);
                pushChannel.send(message);
            } else {
                TapLogger.info(TAG, "send IMData failed, channel is not connected, " + message);
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
}
