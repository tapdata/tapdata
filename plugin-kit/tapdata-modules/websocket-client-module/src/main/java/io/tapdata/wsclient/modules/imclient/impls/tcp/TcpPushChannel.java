package io.tapdata.wsclient.modules.imclient.impls.tcp;
//package modules.imclient.impls.tcp;
//
//import modules.imclient.impls.PushChannel;
//import modules.imclient.utils.LoggerEx;
//
//import java.io.DataInputStream;
//import java.io.DataOutputStream;
//import java.io.IOException;
//import java.net.InetSocketAddress;
//import java.net.Socket;
//import java.nio.channels.SocketChannel;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
//public class TcpPushChannel extends PushChannel {
//
//    public static final short ENCODE_JSON = 1;
//    public static final short ENCODE_JSON_GZIP = 2;
//
//
//    public static final short TYPE_OUT_PUSHMESSAGE = 10;
//    public static final short TYPE_OUT_NOTIFICATIONEVENT = 20;
//    public static final short TYPE_OUT_RESULT = 100;
//
//    private static final int TIMEOUT = 120000;
//
//    private InetSocketAddress isa;
//    private SocketChannel client;
//
//    private Socket socket = null;
//    private DataInputStream socketIs;
//    private DataOutputStream socketOs;
//
//    private boolean isRunning;
//    private boolean isChannelClosed = false;
//
//    private PingThread pingThread;
//    private ReadThread readThread;
//    private WriteThread writeThread;
//    private final ExecutorService mExecutorService;
//
//    public TcpPushChannel() {
//        mExecutorService = Executors.newFixedThreadPool(3);
//    }
//
//    private synchronized boolean connect() {
//        if (client == null || !client.isConnected()) {
//            String host = RuntimeStorage.getInstance().getHost();
//            Integer port = RuntimeStorage.getInstance().getTcpPort();
//            try {
//                LoggerEx.info("connecting " + host + ":" + port);
//
//
//                InetSocketAddress isa = new InetSocketAddress(host, port);
//                Socket socket = new Socket();
//                socket.connect(isa);
//
//                LoggerEx.info("connected to " + host + ":" + port);
//
//                this.socketIs = new DataInputStream(socket.getInputStream());
//                this.socketOs = new DataOutputStream(socket.getOutputStream());
//
//                LoggerEx.info("Input/Output Stream opened to " + host + ":" + port);
//
//                LoggerEx.info("=======TCP===连接成功=======");
//				/*
//				ANDROID 7.0以上存在问题
//				client = SocketChannel.open();
//				this.isa = new InetSocketAddress(host, port);
//				client.configureBlocking(true);
//				client.connect(this.isa);
//				this.socket = client.socket();
////				socket.setSoTimeout(100);
//
//				LoggerEx.info("connected to " + host + ":" + port);
//
//				socketIs = new DataInputStream(this.socket.getInputStream());
//				this.socketOs = new DataOutputStream(this.socket.getOutputStream());
//
//				LoggerEx.info( "Input/Output Stream opened to " + host + ":" + port);
//
//                LoggerEx.info("=======TCP===连接成功=======");*/
//                return true;
//            } catch (Throwable t) {
//                LoggerEx.error("connect:", t);
//                t.printStackTrace();
//                disconnect();
//                LoggerEx.error("PushControl Channel connect failed " + t.getMessage());
//            }
//        }
//        return (client != null && client.isConnected());
//    }
//
//    private synchronized void disconnect() {
//        LoggerEx.info("close Socket instance.");
//        if (socket != null) {
//            try {
//                socket.close();
//            } catch (IOException e) {
//                LoggerEx.error("", e);
//            }
//            socket = null;
//        }
//        LoggerEx.info("close Socket Channel.");
//        if (client != null) {
//            try {
//                client.close();
//            } catch (Exception e) {
//                LoggerEx.error("", e);
//            }
//            client = null;
//        }
//
//        LoggerEx.info("close Socket iostream.");
//        if (socketIs != null && socketOs != null) {
//            try {
//                socketIs.close();
//            } catch (Exception e) {
//                LoggerEx.error("", e);
//            }
//            try {
//                socketOs.close();
//            } catch (IOException e) {
//                LoggerEx.error("", e);
//            }
//            socketIs = null;
//            socketOs = null;
//        }
//    }
//
//    private void sendACKMessage(Event event) throws IOException, JSONException {
//        ClientACKPackage ackPackage = new ClientACKPackage();
//        ackPackage.setIds(new String[]{event.getId()});
//
//        LoggerEx.info("ACKMessage is " + ackPackage);
//
//
//        send(ackPackage);
//    }
//
//    private void registerChannel() throws CoreException, IOException, JSONException {
//
//        PackageManager packageManager = context.getPackageManager();
//        Integer version = null;
//        try {
//            PackageInfo packInfo = packageManager.getPackageInfo(context.getPackageName(), 0);
//            version = packInfo.versionCode;
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        if (version == null) {
//            throw new CoreException(0, "Version is empty, can not start push channel.");
//        }
//
//        IdentityPackage identity = new IdentityPackage();
//        identity.setSid(RuntimeStorage.getInstance().getSid());
////		identity.setAffiliate(((Context)Runtime.create().getAnyContext()).getString(R.string.app_affiliate));
//        identity.setVersion(version);
//        identity.setDeviceToken(RuntimeStorage.getInstance().getDeviceToken());
//        identity.setUid(RuntimeStorage.getInstance().getUser().getId());
//        identity.setTerminal(ChatConstants.TERMINAL);
//
//        LoggerEx.info("Register Identity " + identity);
//        socketOs.write(RuntimeStorage.getInstance().getServer().getBytes());
//        send(identity);
//
//        ResultPackage result = (ResultPackage) receiveData();
//        if (result.getCode() != ResultPackage.CODE_SUCCESS) {
//            throw new CoreException(result.getCode(), "PushControl Channel register failed, " + result.getResultDescription());
//        }
//    }
//
//    private void send(OutgoingPackage outgoingPackage) throws IOException {
//        if (outgoingPackage != null && outgoingPackage.getType() != null) {
//            JSONObject jsonObj = outgoingPackage.toJSONObject();
//            if (jsonObj != null) {
//                String jsonString = jsonObj.toString();
//                byte[] jsonData = jsonString.getBytes("utf8");
//                socketOs.writeInt(jsonData.length);
//                socketOs.writeShort(outgoingPackage.getType());
//                socketOs.writeShort(ENCODE_JSON);
//                socketOs.write(jsonData);
//            }
//        }
//    }
//
//    @Override
//    public void ping() {
//        if (pingThread != null) {
//            pingThread.ping();
//        }
//    }
//
//    private class PingThread implements Runnable {
//        public final Long WAIT_PERIOD = TimeUnit.SECONDS.toMillis(8);
//        public final Long PING_PERIOD = TimeUnit.MINUTES.toMillis(15);
//        //		public final Long PING_PERIOD = TimeUnit.SECONDS.toMillis(10);
//        private Long touch;
//        private Long nextExpireTime = null;
//
//        public final int STATUS_IDLE = 1;
//        public final int STATUS_PING = 10;
//        public final int STATUS_WAITPINGRESULT = 20;
//        private int status = STATUS_IDLE;
//
//        private Object waitLock = new Object();
//
//        public PingThread() {
//
//            touch = System.currentTimeMillis();
//        }
//
//        @Override
//        public void run() {
//            while (!isChannelClosed) {
//                synchronized (waitLock) {
//                    if (status == STATUS_WAITPINGRESULT) {
//                        try {
//                            waitLock.wait(WAIT_PERIOD / 2);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                    } else {
//                        try {
////							waitLock.wait(60000L);
//                            waitLock.wait(10000L);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                    }
//                }
//
////				if(nextExpireTime != null && nextExpireTime < System.currentTimeMillis()) {
////					channelClosed(Cause.CAUSE_READWRITE_FAILED);
////					break;
////				}
//                if (ActivityLifecycleHandler.getInstance().getCurrentActivity() != null) {
//                    touch();
//                }
//                if (PING_PERIOD + touch < System.currentTimeMillis()) {
//                    if (nextExpireTime == null && status == STATUS_IDLE) {
//                        ping();
//                    }
//                }
//            }
//            LoggerEx.info("Ping thread finished, " + this);
//        }
//
//        public void ping() {
//            synchronized (this) {
//                if (nextExpireTime == null && status == STATUS_IDLE) {
//                    status = STATUS_PING;
//                    nextExpireTime = System.currentTimeMillis() + WAIT_PERIOD;
//                    wakeUpSending();
//                }
//            }
//        }
//
//        public void wakeUpForPingResult() {
//            status = pingThread.STATUS_WAITPINGRESULT;
//            synchronized (waitLock) {
//                waitLock.notify();
//            }
//        }
//
//        public void pingResultReceived(ResultPackage resultPackage) {
//            synchronized (this) {
//                if (nextExpireTime != null && status == STATUS_WAITPINGRESULT) {
//                    status = STATUS_IDLE;
//                    nextExpireTime = null;
//                }
//            }
//
//            Integer code = resultPackage.getCode();
//            if (code != null && code == 1185) {
//                LoggerEx.info("Switching service to PushNotificationService now. ");
//                service.terminate();
//                Intent intent = new Intent(context, PushNotificationService.class);
//                intent.putExtra("start", true);
//                context.startService(intent);
//            }
//        }
//
//        public void touch() {
//            touch = System.currentTimeMillis();
//        }
//    }
//
//    private class ReadThread implements Runnable {
//
//        @Override
//        public void run() {
//            handle(new HandlerEx() {
//                @Override
//                public void handle() throws Throwable {
//                    while (isRunning) {
//                        IncomingPackage incomingPackage = receiveData();
//                        pingThread.touch();
//                        if (incomingPackage instanceof ResultPackage) {
//                            ResultPackage resultPackage = (ResultPackage) incomingPackage;
//                            String reply = resultPackage.getReply();
//                            if (reply != null && reply.equals(String.valueOf(PingPackage.PING_TYPE))) {
//                                pingThread.pingResultReceived(resultPackage);
//                                continue;
//                            }
//                            String clientId = resultPackage.getClientId();
//                            if (clientId != null) {
//                                Map<String, ReceiveMessagePackageHelper> waitResultMap = service.getWaitResultMap();
//                                //直接删掉等待Map里的数据
//                                ReceiveMessagePackageHelper helper = waitResultMap.remove(clientId);
//                                if (helper != null) {
//                                    service.getSendingMessageIds().remove(clientId);
//                                    resultPackage.setResultFor(helper.getMessagePackage());
//
//                                    try {
//                                        helper.cancel();//停止超时计时
//                                    } catch (Throwable t) {
//                                        t.printStackTrace();
//                                    }
//                                }
//                            }
//                        }
//                        try {
//                            handleEvents(incomingPackage);
//                        } catch (Throwable t) {
//                            // TODO: 2018/8/1
//                            Logger.w("in handleEvents sm wrong happend!!!");
//                        }
//                        if (incomingPackage instanceof Event && ((Event) incomingPackage).isNeedACK()) {
//                            sendACKMessage((Event) incomingPackage);
//                        }
//                    }
//                }
//            });
//            LoggerEx.info("Read thread finished, " + this);
//        }
//    }
//
//    public class ReceiveMessagePackageHelper extends TimerTask {
//        private ReceiveMessagePackage messagePackage;
//        private final Object cancelLock = new Object();
//        private boolean isCanceled = false;
//
//        @Override
//        public boolean cancel() {
//            synchronized (cancelLock) {
//                isCanceled = true;
//                try {
//                    return super.cancel();
//                } catch (Throwable t) {
//                    t.printStackTrace();
//                    return false;
//                }
//            }
//        }
//
//        public ReceiveMessagePackageHelper(ReceiveMessagePackage messagePackage) {
//            this.messagePackage = messagePackage;
//        }
//
//        @Override
//        public void run() {
//            if (!isCanceled) {
//                synchronized (cancelLock) {
//                    if (!isCanceled) {
//                        Map<String, ReceiveMessagePackageHelper> waitResultMap = service.getWaitResultMap();
//                        Long sendTime = messagePackage.getSendTime();
//                        //判断重发逻辑
//                        if (sendTime != null && sendTime + ReceiveMessagePackage.RETRY_TIME > System.currentTimeMillis()) {
//                            service.send(messagePackage);
//                        } else {
//                            //如果不重发， 就送等待Map里删掉。
////                            ReceiveMessagePackageHelper receiveMessagePackageHelper = waitResultMap.get(messagePackage.getClientId());
//                            waitResultMap.remove(messagePackage.getClientId());
//                            service.getSendingMessageIds().remove(messagePackage.getClientId());
//                            //模拟发送失败的ResultPackage
//                            ResultPackage resultPackage = new ResultPackage();
//                            resultPackage.setResultFor(messagePackage);
//                            resultPackage.setType(ResultPackage.EVENTTYPE_RESULT);
//                            resultPackage.setClientId(messagePackage.getClientId());
//                            resultPackage.setCode(PushChannel.ERRORCODE_TIMEOUT);
//                            resultPackage.setResultDescription("Timeout");
//                            try {
//                                handleEvents(resultPackage);
//                            } catch (Throwable t) {
//                                t.printStackTrace();
//                                Logger.w(" handle fake result package " +
//                                        resultPackage + " failed, " + t.getMessage());
//                            }
//                        }
//                    }
//                }
//            }
//        }
//
//        public ReceiveMessagePackage getMessagePackage() {
//            return messagePackage;
//        }
//    }
//
//    private class WriteThread implements Runnable {
//        @Override
//        public void run() {
//            handle(new HandlerEx() {
//                @Override
//                public void handle() throws Throwable {
//                    Map<String, ReceiveMessagePackageHelper> waitResultMap = service.getWaitResultMap();
//                    Queue<OutgoingPackage> sendingQueue = service.getSendingQueue();
//                    while (isRunning) {
//                        OutgoingPackage op = sendingQueue.poll();
//                        if (pingThread != null && pingThread.status == pingThread.STATUS_PING) {
////							pingThread.status = pingThread.STATUS_WAITPINGRESULT;
//                            pingThread.wakeUpForPingResult();
//                            send(new PingPackage());
//                        }
//                        if (op != null) {
//                            if (op.getType() != null && op.getType() == ReceiveMessagePackage.RECEIVEMESSAGE_TYPE) {
//                                ReceiveMessagePackage messagePackage = (ReceiveMessagePackage) op;
//                                String clientId = messagePackage.getClientId();
//                                if (clientId != null) {
////									Log.v("hhhhh","TimeUnit.SECONDS.toMillis(60));//这么多秒后重试");
//                                    ReceiveMessagePackageHelper timerTask = new ReceiveMessagePackageHelper(messagePackage);
//                                    TimerEx.schedule(timerTask, TimeUnit.SECONDS.toMillis(60));//这么多秒后重试
//                                    waitResultMap.put(clientId, timerTask);
//                                }
//                            }
//                            send(op);
//                        } else {
//                            synchronized (writeThread) {
//                                try {
//                                    writeThread.wait(TimeUnit.HOURS.toMillis(1));
//                                } catch (InterruptedException e) {
//                                    LoggerEx.error(e, "");
//                                }
//                            }
//                        }
//                    }
//                }
//            });
//            LoggerEx.info("Write thread finished, " + this);
//        }
//    }
//
//    public void wakeUpSending() {
//        if (writeThread != null) {
//            synchronized (writeThread) {
//                writeThread.notify();
//            }
//        }
//    }
//
//    @Override
//    public synchronized void stop() {
//        if (!isChannelClosed) {
//            isChannelClosed = true;
//        }
//        isRunning = false;
//        wakeUpSending();
//        disconnect();
//    }
//
//    @Override
//    public synchronized void start() {
//        if (connect()) {
//            handle(() -> {
//                registerChannel();
//                isRunning = true;
//                readThread = new ReadThread();
//                pingThread = new PingThread();
//                writeThread = new WriteThread();
//                mExecutorService.execute(readThread);
//                mExecutorService.execute(writeThread);
//                mExecutorService.execute(pingThread);
//               /* new Thread(readThread,"ReadThread").start();
//                new Thread(writeThread,"WriteThread").start();
//                new Thread(pingThread,"PingThread").start();*/
//            });
//        } else {
//            channelClosed(Cause.CAUSE_CONNECT_FAILED);
//        }
//    }
//
//    private synchronized void channelClosed(int cause) {
//        if (!isChannelClosed) {
//            isChannelClosed = true;
//            try {
//                if (channelCloseListener != null)
//                    channelCloseListener.channelClosed(cause);
//            } finally {
//                stop();
//                Map<String, ReceiveMessagePackageHelper> map = service.getWaitResultMap();
//                Set<String> keys = map.keySet();
//                for (String key : keys) {
//                    ReceiveMessagePackageHelper helper = map.get(key);
//                    if (helper != null) {
//                        try {
//                            helper.cancel();
//                        } catch (Throwable t) {
//                            t.printStackTrace();
//                        }
//                        if (helper.isCanceled)
//                            service.send(helper.messagePackage);
//                    }
//                }
//                map.clear();
//            }
//        }
//    }
//
//    private void handle(HandlerEx handler) {
//        try {
//            handler.handle();
//        } catch (Throwable t) {
//            LoggerEx.error(t, "");
//            if (t instanceof IOException) {
//                channelClosed(Cause.CAUSE_READWRITE_FAILED);
//            } else if (t instanceof CoreException) {
//                CoreException te = (CoreException) t;
//                channelClosed(Cause.getCause(te.getCode()));
//            } else {
//                channelClosed(Cause.CAUSE_LOGIC_FAILED);
//            }
//        }
//
//    }
//
//    @SuppressLint("CheckResult")
//    private IncomingPackage receiveData() throws IOException, JSONException {
//        LoggerEx.info("waiting data.");
////		monitorRead(pack, socketIs);
//
//        int length = socketIs.readInt();
//        short packageType = socketIs.readShort();
//        short encodeTyep = socketIs.readShort();
//        byte[] data = null;
//        switch (encodeTyep) {
//            case ENCODE_JSON:
//                data = new byte[length];
//                IOUtils.readFully(socketIs, data);
//                break;
//            default:
//                throw new IOException("Received unexpected data encoding, " + encodeTyep);
//        }
//
//        LoggerEx.info("receiving data, length=" + data.length);
//        String dataString = new String(data, "utf8");
//        JSONObject jsonObj = new JSONObject(dataString);
//        IncomingPackage incomingPackage = null;
//        switch (packageType) {
//            case TYPE_OUT_PUSHMESSAGE:
//            case TYPE_OUT_NOTIFICATIONEVENT:
//                String eventType = jsonObj.getString(Event.EVENT_TYPE);
//                if (eventType != null) {
//                    Class<? extends IncomingPackage> incomeClass = Config.getInstance().getIncomingPackageMap().get(eventType);
//                    if (incomeClass != null) {
//                        try {
//                            incomingPackage = incomeClass.newInstance();
//                            incomingPackage.fromJSONObject(jsonObj);
//                        } catch (InstantiationException e) {
//                            e.printStackTrace();
//                            LoggerEx.error("Receive incoming package " + incomeClass
//                                    + " new instance failed, " + e.getMessage());
//                        } catch (IllegalAccessException e) {
//                            e.printStackTrace();
//                            LoggerEx.error("Receive incoming package " + incomeClass
//                                    + " new instance failed: " + e.getMessage());
//                        }
//                    } else {
//                        try {
//                            incomingPackage = Event.class.newInstance();
//                            incomingPackage.fromJSONObject(jsonObj);
//                        } catch (InstantiationException e) {
//                            e.printStackTrace();
//                            LoggerEx.error("Receive incoming package " + Event.class
//                                    + " new instance failed, " + e.getMessage());
//                        } catch (IllegalAccessException e) {
//                            e.printStackTrace();
//                            LoggerEx.error("Receive incoming package " + Event.class
//                                    + " new instance failed: " + e.getMessage());
//                        }
//                    }
//                }
//                //TODO
//                break;
//            case TYPE_OUT_RESULT:
//                incomingPackage = new ResultPackage();
//                incomingPackage.setType(ResultPackage.EVENTTYPE_RESULT);//为给ResultPackage加上type的特殊化
//                incomingPackage.fromJSONObject(jsonObj);
//                break;
//            default:
//                throw new IOException("Received unexpected data chatType, " + packageType);
//        }
//
//        return incomingPackage;
//    }
//
//	/*
//	private void monitorRead(final HailPack pack, final DataInputStream is) throws IOException {
//		final long time = System.currentTimeMillis();
//		LogEx.d("monitorRead", "start time for timeout " + time);
//		final int[] lock = new int[0];
//		Thread thread = new Thread(new Runnable() {
//			@Override
//			public void run() {
//				synchronized (lock) {
//					try {
//						LogEx.d("monitorRead", "start waiting timeout " + TIMEOUT);
//						lock.wait(TIMEOUT);
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
//					long current = System.currentTimeMillis();
//					if(current - time >= TIMEOUT) {
//						LogEx.d("monitorRead", "Will stop push channel " + (current - time));
//						TcpPushChannel.this.stop();
//					} else {
//						LogEx.d("monitorRead", "Not timeout, thread dying " + Thread.currentThread());
//					}
//				}
//			}
//		});
//		thread.start();
//		LogEx.d("monitorRead", "start reading data, thread " + thread);
//		try {
//			pack.resurrect(is);
//		} finally {
//			LogEx.d("monitorRead", "pack.resurrect done");
//			synchronized (lock) {
//				lock.notify();
//			}
//		}
//	}*/
//
//}
