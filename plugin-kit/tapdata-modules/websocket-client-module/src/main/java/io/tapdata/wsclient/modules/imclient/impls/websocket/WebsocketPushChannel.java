package io.tapdata.wsclient.modules.imclient.impls.websocket;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.wsclient.modules.imclient.impls.MonitorThread;
import io.tapdata.wsclient.modules.imclient.impls.PushChannel;
import io.tapdata.wsclient.modules.imclient.impls.data.Data;
import io.tapdata.wsclient.modules.imclient.impls.data.Identity;
import io.tapdata.wsclient.modules.imclient.impls.data.IncomingData;
import io.tapdata.wsclient.modules.imclient.impls.data.IncomingMessage;
import io.tapdata.wsclient.modules.imclient.impls.data.Ping;
import com.dobybros.tccore.promise.ErrorHandler;
import com.dobybros.tccore.promise.Handler;
import com.dobybros.tccore.promise.Promise;
import com.dobybros.tccore.promise.ThenHandler;
import io.tapdata.wsclient.utils.EventManager;
import io.tapdata.wsclient.utils.HttpUtils;
import io.tapdata.wsclient.utils.LoggerEx;
import io.tapdata.wsclient.utils.TimerEx;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;


public class WebsocketPushChannel extends PushChannel {
    private static final String TAG = WebsocketPushChannel.class.getSimpleName();
    private Integer wsPort;
    private String host;
    private String server;
    private String sid;

    private final String appId = "aculearn";
    private final int sdkVersion = 1;
    private final String key = "akdafasdf";
    private final String deviceToken = "dt";

    public static final short encodeVersion = 1;
    public static final short version = 1;

    private Channel channel;
    private EventManager eventManager;
    boolean isConnected = false;

    ScheduledFuture pingFuture;

    @Override
    public void stop() {
        LoggerEx.info(TAG, "aaaaaaaaaaaaa");
        if(pingFuture != null) {
            pingFuture.cancel(true);
        }
        LoggerEx.info(TAG, "bbbbbbbbbbbbb");
        if(channel != null)
            channel.disconnect();
        LoggerEx.info(TAG, "ccccccccccccc");
    }

    class IOErrorHandler implements ErrorHandler {
        private String title;
        private IOErrorHandler(String title) {
            this.title = title;
        }
        @Override
        public void handle(Throwable throwable) throws IOException {
            LoggerEx.error(title + " failed, " + throwable.getMessage(), throwable);
            if(throwable instanceof IOException) {
                throw (IOException)throwable;
            }
            throw new IOException(title + " failed, " + throwable.getMessage(), throwable);
        }
    }
    @Override
    public void start() {
        if(imClient == null)
            throw new NullPointerException("IMClient is needed for creating channels.");
        eventManager = EventManager.getInstance();
        LoggerEx.info(TAG, "PushChannel started");
        Promise.handle((Handler<Void>) () -> {
            login();
            LoggerEx.info(TAG, "Login successfully, " + host + " " + wsPort + " " + server + " " + sid);
            return null;
        }).then((ThenHandler<Void, Void>) param -> {
            connectWS("wss", host, wsPort, null);
            LoggerEx.info(TAG, "WS connected successfully, " + host + " " + wsPort + " " + server + " " + sid);
            return null;
        }).error(throwable -> {
            LoggerEx.error(TAG, "WS connected failed, " + host + " " + wsPort + " " + server + " " + sid);
            eventManager.sendEvent(imClient.getPrefix() + ".status", new ChannelStatus(this, ChannelStatus.STATUS_DISCONNECTED, MonitorThread.CHANNEL_ERRORS_LOGIN_FAILED));
        });

    }

    @Override
    public void selfCheck() {

    }

    @Override
    public void sendData(IncomingData data) throws IOException {
        sendDataPrivate(data);
    }

    @Override
    public void sendMessage(IncomingMessage message) {
    }

    private void login() throws IOException {
        String loginUrl = imClient.getLoginUrl();

        JSONObject loginObj = new JSONObject();
        loginObj.put("account", imClient.getUserId());
        loginObj.put("terminal", imClient.getTerminal());
        loginObj.put("appId", appId);
        loginObj.put("service", imClient.getService());
        Map<String, String> headers = new HashMap<>();
//        headers.put("imapitoken", imClient.getToken());
        headers.put("classtoken", imClient.getToken());
        JSONObject data = HttpUtils.post(loginUrl, loginObj, headers);
        wsPort = data.getInteger("wsport");
        host = data.getString("host");
        server = data.getString("s");
        sid = data.getString("sid");

        if(wsPort == null || host == null || server == null || sid == null) {
            throw new IOException("Illegal parameters for wsPort " + wsPort + " host " + host + " server " + server + " sid " + sid);
        }
    }

    private void connectWS(String protocol, final String host, final int port, ChannelHandlerContext ctx) throws IOException {

        if (!"ws".equalsIgnoreCase(protocol) && !"wss".equalsIgnoreCase(protocol)) {
            throw new IOException("Only WS(S) is supported.");
        }

        final boolean ssl = "wss".equalsIgnoreCase(protocol);
        final SslContext sslCtx;
        if (ssl) {
            sslCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        } else {
            sslCtx = null;
        }

        URI uri = null;
        try {
            uri = new URI(protocol + "://" + host + ":" + port);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        if(uri == null)
            throw new IOException("uri illegal, " + protocol + "://" + host + ":" + port);

        EventLoopGroup group = new NioEventLoopGroup();
        final WebSocketClientHandler handler = new WebSocketClientHandler(ctx, WebSocketClientHandshakerFactory
                .newHandshaker(uri, WebSocketVersion.V13, null, false, new DefaultHttpHeaders()));
        handler.pushChannel = this;

        Bootstrap b = new Bootstrap();
        b.option(ChannelOption.SO_KEEPALIVE,true)
                .option(ChannelOption.TCP_NODELAY,true)
//                .option(ChannelOption.SO_BACKLOG,1024*1024*10)
                .group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ChannelPipeline p = ch.pipeline();
                if (sslCtx != null) {
                    p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                }
                p.addLast(new HttpClientCodec(), new HttpObjectAggregator(8192), handler);
            }
        });

        try {
            channel = b.connect(uri.getHost(), port).sync().channel();
            handler.handshakeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new IOException("Connect and handshake websocket failed, " + e.getMessage(), e);
        }
        sendServer();
        LoggerEx.info(TAG, "connectWS: "+"sendServer");
        Identity identity = new Identity();
//        identity.setSessionId(sid);
        identity.setSdkVersion(sdkVersion);
        identity.setTerminal(imClient.getTerminal());
        identity.setService(imClient.getService());
        identity.setAppId(appId);
        identity.setCode(sid);
        identity.setDeviceToken(deviceToken);
        identity.setSessionId(sid);
        identity.setUserId(imClient.getUserId());
        identity.setKey(key);
        sendIdentity(identity);
        LoggerEx.info(TAG, "connectWS: "+"sendIdentity"+identity);
    }

    private void sendServer() throws IOException {
        if(channel == null) return;
        ByteBuf byteBuf = Unpooled.directBuffer(6);
        byteBuf.writeBytes(server.getBytes("utf8"));

        channel.writeAndFlush(new BinaryWebSocketFrame(byteBuf));
    }

    private void sendIdentity(Identity data) throws IOException {
        if(channel == null) return;

        byte[] bytes = data.getData();
        if(bytes == null) {
            data.persistent();
            bytes = data.getData();
        }
        if(bytes == null)
            throw new IOException("Persistent identity " + data.getClass() + " failed");

        ByteBuf byteBuf = Unpooled.directBuffer(1 + 2 + 1 + 1 + 4 + bytes.length);
        byteBuf.writeByte(version); //version
        byteBuf.writeShort(encodeVersion); //encodeVersion
        byteBuf.writeByte(data.getType());
        byteBuf.writeByte(Data.ENCODE_PB); //encode
        byteBuf.writeInt(bytes.length);
        byteBuf.writeBytes(bytes);

        channel.writeAndFlush(new BinaryWebSocketFrame(byteBuf));
    }

    boolean isAlive() {
        return channel != null && isConnected;
    }

    public void ping() throws IOException {
        if(!isAlive()) return;
        if(pingFuture == null) {
            Ping ping = new Ping();
            ping.setId("ping");
            sendDataPrivate(ping);
            LoggerEx.info(TAG, "ping");
            pingFuture = TimerEx.scheduleInSeconds(() -> {
                pingFuture = null;
                stop();
                LoggerEx.info(TAG, "Stop channel because of ping timeout");
            }, 10);
        }
    }

    private void sendDataPrivate(Data data) throws IOException {
        if(!isAlive()) return;

        byte[] bytes = data.getData();
        if(bytes == null) {
            data.persistent();
            bytes = data.getData();
        }
        if(bytes == null)
            throw new IOException("Persistent data " + data.getClass() + " failed");


        ByteBuf byteBuf = Unpooled.directBuffer(1 + 4 + bytes.length);
        byteBuf.writeByte(data.getType());
        byteBuf.writeInt(bytes.length);
        byteBuf.writeBytes(bytes);

        channel.writeAndFlush(new BinaryWebSocketFrame(byteBuf));
    }

    public Integer getWsPort() {
        return wsPort;
    }

    public void setWsPort(Integer wsPort) {
        this.wsPort = wsPort;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }
}
