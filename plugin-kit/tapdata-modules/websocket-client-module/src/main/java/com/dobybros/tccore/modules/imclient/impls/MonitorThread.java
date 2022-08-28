package com.dobybros.tccore.modules.imclient.impls;

import com.dobybros.tccore.modules.imclient.data.IMData;
import com.dobybros.tccore.modules.imclient.data.DataHelper;
import com.dobybros.tccore.modules.imclient.data.IMMessage;
import com.dobybros.tccore.modules.imclient.data.IMResult;
import com.dobybros.tccore.modules.imclient.impls.data.IncomingData;
import com.dobybros.tccore.modules.imclient.impls.data.OutgoingData;
import com.dobybros.tccore.modules.imclient.impls.data.Ping;
import com.dobybros.tccore.modules.imclient.impls.data.Result;
import com.dobybros.tccore.modules.imclient.impls.websocket.ChannelStatus;
import com.dobybros.tccore.utils.EventManager;
import com.dobybros.tccore.utils.LoggerEx;

import io.tapdata.entity.error.CoreException;
import org.apache.commons.lang3.NotImplementedException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;

import static com.dobybros.tccore.modules.imclient.impls.data.Const.CONTENTENCODE_JSON;

public class MonitorThread<T extends PushChannel> extends Thread {
    public static final int CHANNEL_ERRORS_LOGIN_FAILED = 110;

    public static final int STATUS_IDLE = 1;
    public static final int STATUS_STARTED = 2;
    public static final int STATUS_TERMINATED = 4;
    private static final String TAG = MonitorThread.class.getSimpleName();

    public int getStatus() {
        return status;
    }

    private int status = STATUS_IDLE;

    private T pushChannel;
    private final int[] lock = new int[0];
    private final int[] channelLock = new int[0];

    private int count = 0;
    private final long[] idleTimes = new long[]{RETRY_TIME, 5000, 6000, 7000, 8000, 9000, 10000, 20000, 30000, 45000, 60000, 600000};
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
            LoggerEx.info(TAG, "MonitorThread already started, notify or self check");
        } else if(status == STATUS_IDLE) {
            status = STATUS_STARTED;
            if(!this.isAlive()) {
                super.start();
                LoggerEx.info(TAG, "MonitorThread started");
            }
        }
    }

    public synchronized final void terminate() {
        if(status == STATUS_TERMINATED)
            return;
        LoggerEx.info(TAG, "MonitorThread terminated");

        status = STATUS_TERMINATED;
        LoggerEx.info(TAG, "1111111111111");
        if(pushChannel != null)
            pushChannel.stop();
        LoggerEx.info(TAG, "2222222222222");
        synchronized (lock) {
            idleTime = 0;
            count = 0;
            lock.notifyAll();
        }
        LoggerEx.info(TAG, "333333333333333");
        eventManager.unregisterEventListener(this);
        LoggerEx.info(TAG, "444444444444");
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
        LoggerEx.info(TAG, "MonitorThread restart channel, " + (hurry ? "" : "no ") + "hurry");
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

    void handleMessageSendFailed(IMData imData, String message) {
        if(imData == null) return ;
        LoggerEx.info(TAG, "send message failed, " + message + " imdata " + imData);
        ResultListenerWrapper wrapper = imClient.resultMap.get(imData.getId());
        if(wrapper != null) {
            wrapper.getResultHandler().completeExceptionally(new IOException(message));
            imClient.resultMap.remove(imData.getId());
        }
    }

    void sendMessageFromQueue() {
        if(!imClient.messageQueue.isEmpty() && pushChannel != null) {
            IMData imData = null;
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
        LoggerEx.info(TAG, "Monitor Thread is running");
        eventManager.registerEventListener(this, imClient.getPrefix() + ".status", (EventManager.EventListener<ChannelStatus>) (eventType, channelStatus) -> {
            LoggerEx.info(TAG, "status changed, " + channelStatus);
            switch (channelStatus.getType()) {
                case ChannelStatus.STATUS_CONNECTED:
                    resetIdelTimes();
                    wakeupForMessage();
                    break;
                case ChannelStatus.STATUS_DISCONNECTED:
                    if(pushChannel != null && channelStatus.getPushChannel().equals(pushChannel)) {
                        shiftIdleTimes();
                        restartChannel(false);
                    }
                    break;
                case ChannelStatus.STATUS_OFFLINEMESSAGECONSUMED:
                    break;
                case ChannelStatus.STATUS_BYE:
                case ChannelStatus.STATUS_KICKED:
                    resetIdelTimes();
                    if(pushChannel != null && channelStatus.getPushChannel().equals(pushChannel)) {
                        terminate();
                    }
                    break;
            }
        });
        eventManager.registerEventListener(this, imClient.getPrefix() + ".data", (EventManager.EventListener<OutgoingData>) (eventType, message) -> {
            if(message != null && message.getContentType() != null) {
                Class<? extends IMData> dataClass = imClient.contentTypeClassMap.get(message.getContentType());
                if(dataClass != null) {
                    IMData imData = IMData.buildReceivingData(message, dataClass);

                    if(imData != null) {
                        LoggerEx.info(TAG, "PushChannel receive imdata " + imData);
                        eventManager.sendEvent(imClient.getPrefix() + ".imdata", imData);
                        eventManager.sendEvent(imClient.getPrefix() + ".imdata." + imData.getContentType(), imData);
                    }
                }
            }
        });
        eventManager.registerEventListener(this, imClient.getPrefix() + ".result", (EventManager.EventListener<Result>) (eventType, result) -> {
//            if(inteceptErrorResult(result)) {
//               return;
//            }
            IMResult imResult = DataHelper.fromResult(result);
            String id = result.getForId();
            if(id != null) {
                ResultListenerWrapper wrapper = imClient.resultMap.get(id);
                if(wrapper != null) {
                    if(imResult.getCode() == 1) {
                        wrapper.getResultHandler().complete(imResult);
                    } else {
                        byte[] data = imResult.getData();
                        String dataStr = null;
                        if(data != null) {
                            try {
                                dataStr = new String(data, "utf8");
                            } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                            }
                        }
                        wrapper.getResultHandler().completeExceptionally(new CoreException(imResult.getCode(), imResult.getDescription(), dataStr));
                    }
                } else {
                    eventManager.sendEvent(imClient.getPrefix() + ".imresult", imResult);
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
                            pushChannel.start();
                        }
                    }
                } else {
                    sendMessageFromQueue();
                    synchronized (lock) {
                        try {
                            lock.wait(7000L);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        synchronized (channelLock) {
                            if(pushChannel != null) {
                                pushChannel.ping();
                            }
                        }
                    }
//                    LoggerEx.info( "PushControl thread wake up for creating new channel " + pushChannel);
                }
            } catch (Exception e) {
                e.printStackTrace();
                LoggerEx.error("error occured " + e.getMessage(), e);
            }
        }
//		try {
//		} finally {
//			status = STATUS_TERMINATED;
//		}
    }

    boolean send(IMData message) throws IOException {
        if(message instanceof IMMessage) {
            throw new NotImplementedException("not implemented");
        } else {
            IncomingData incomingData = new IncomingData();
            byte[] bytes = DataHelper.toJsonBytes(message);
            incomingData.setContent(bytes);
            incomingData.setContentEncode(CONTENTENCODE_JSON);
            incomingData.setContentType(message.getContentType());
            incomingData.setId(message.getId());
            incomingData.setService(imClient.getService());

            synchronized (channelLock) {
                if(pushChannel != null) {
                    LoggerEx.info(TAG, "send IMData " + message);
                    pushChannel.sendData(incomingData);
                } else {
                    LoggerEx.info(TAG, "send IMData failed, channel is not connected, " + message);
                    return false;
                }
                return true;
            }
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
