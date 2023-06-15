package io.tapdata.wsclient.modules.imclient.impls.websocket;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.netty.channel.*;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.modules.api.net.data.*;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.utils.TapEngineUtils;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.wsclient.modules.imclient.impls.MonitorThread;
import io.tapdata.wsclient.modules.imclient.impls.PushChannel;
import io.tapdata.wsclient.utils.EventManager;
import io.tapdata.wsclient.utils.HttpUtils;
import io.tapdata.wsclient.utils.TimerEx;

import java.io.IOException;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Supplier;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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

import javax.net.ssl.SSLException;


public class WebsocketPushChannel extends PushChannel {
    private static final String TAG = WebsocketPushChannel.class.getSimpleName();
    private String protocol;
    private Integer wsPort;
    private String path;
    private String host;
    private String server;
    private String sid;
    private String baseUrl;

    private final int sdkVersion = 1;
    private final String key = "akdafasdf";
    private final String deviceToken = "dt";

    public static final short encodeVersion = 1;
    public static final short version = 1;

    private Channel channel;
    private EventManager eventManager;
    boolean isConnected = false;

    ScheduledFuture<?> pingFuture;

    private EventLoopGroup group;
    @Override
    public void stop() {
        TapLogger.info(TAG, "stopped");
        if(pingFuture != null) {
            CommonUtils.ignoreAnyError(() -> pingFuture.cancel(true), TAG);
        }
        if(channel != null)
            CommonUtils.ignoreAnyError(() -> channel.disconnect(), TAG);
        if(group != null)
            CommonUtils.ignoreAnyError(() -> group.shutdownGracefully(), TAG);
    }

//    class IOErrorHandler implements ErrorHandler {
//        private String title;
//        private IOErrorHandler(String title) {
//            this.title = title;
//        }
//        @Override
//        public void handle(Throwable throwable) throws IOException {
//            TapLogger.error(title + " failed, " + throwable.getMessage(), throwable);
//            if(throwable instanceof IOException) {
//                throw (IOException)throwable;
//            }
//            throw new IOException(title + " failed, " + throwable.getMessage(), throwable);
//        }
//    }
    @Override
    public void start(String baseUrl) {
        this.baseUrl = baseUrl;
        if(imClient == null)
            throw new NullPointerException("IMClient is needed for creating channels.");
        eventManager = EventManager.getInstance();
        TapLogger.info(TAG, "PushChannel started");

        CompletableFuture.supplyAsync((Supplier<Void>) () -> {
            login();
            TapLogger.info(TAG, "Login successfully, " + host + " " + wsPort + " " + server + " " + sid);
            return null;
        }).thenAccept(unused -> {
            connectWS(protocol, host, wsPort, path);
            TapLogger.info(TAG, "WS connected successfully, " + host + " " + wsPort + " " + server + " " + sid);
        }).exceptionally(throwable -> {
            if(group != null)
                group.shutdownGracefully();
            TapLogger.error(TAG, "WS connected failed, " + host + " " + wsPort + " " + server + " " + sid);
            eventManager.sendEvent(imClient.getPrefix() + ".status", new ChannelStatus(this, ChannelStatus.STATUS_DISCONNECTED, MonitorThread.CHANNEL_ERRORS_LOGIN_FAILED, throwable.getMessage()));
            return null;
        });
//        Promise.handle((Handler<Void>) () -> {
//            login();
//            TapLogger.debug(TAG, "Login successfully, " + host + " " + wsPort + " " + server + " " + sid);
//            return null;
//        }).then((ThenHandler<Void, Void>) param -> {
//            connectWS("wss", host, wsPort, null);
//            TapLogger.debug(TAG, "WS connected successfully, " + host + " " + wsPort + " " + server + " " + sid);
//            return null;
//        }).error(throwable -> {
//            TapLogger.error(TAG, "WS connected failed, " + host + " " + wsPort + " " + server + " " + sid);
//            eventManager.sendEvent(imClient.getPrefix() + ".status", new ChannelStatus(this, ChannelStatus.STATUS_DISCONNECTED, MonitorThread.CHANNEL_ERRORS_LOGIN_FAILED));
//        });

    }

    @Override
    public void selfCheck() {

    }

    @Override
    public void send(Data data) {
        if(channel == null) {
            TapLogger.warn(TAG, "Channel not initialized before sending data, {}", data);
            return;
        }

        byte[] bytes = data.getData();
        if(bytes == null) {
            data.persistent();
            bytes = data.getData();
        }
        if(bytes == null)
            throw new CoreException(NetErrors.PERSISTENT_FAILED, "Persistent identity " + data.getClass() + " failed");

        ByteBuf byteBuf;
        if(bytes.length > 0) {
            byteBuf = Unpooled.directBuffer(1 + 1 + bytes.length);
            byteBuf.writeByte(data.getType());
            byteBuf.writeByte(Data.ENCODE_JAVA_CUSTOM_SERIALIZER); //encode
            byteBuf.writeBytes(bytes);
        } else {
            byteBuf = Unpooled.directBuffer(1);
            byteBuf.writeByte(data.getType());
        }
        channel.writeAndFlush(new BinaryWebSocketFrame(byteBuf));
    }

    private void login() {
        JSONObject loginObj = new JSONObject();
        loginObj.put("clientId", imClient.getClientId());
        loginObj.put("terminal", imClient.getTerminal());
        loginObj.put("service", imClient.getService());
        Map<String, String> headers = new HashMap<>();
        JSONObject data = null;
        String jsonStr = Objects.requireNonNull(InstanceFactory.instance(JsonParser.class)).toJson(loginObj);

        if(imClient.getCachedAccessToken() == null) {
            imClient.setCachedAccessToken(refreshAccessToken(baseUrl));
        }
        String theUrl = baseUrl + "?access_token=" + imClient.getCachedAccessToken();
        TapEngineUtils tapEngineUtils = InstanceFactory.instance(TapEngineUtils.class);
        if(tapEngineUtils != null) {
            theUrl = tapEngineUtils.signUrl("POST", theUrl, jsonStr);
        }

        try {
            String finalTheUrl = theUrl;
            if(imClient.getCachedCookie() != null) {
                headers.put("Cookie", imClient.getCachedCookie());
            }
            data = HttpUtils.post(theUrl, jsonStr, headers, (code, message) -> {
                if(code == 401) {
                    String newToken = refreshAccessToken(baseUrl);
                    imClient.setCachedAccessToken(newToken);
                }
                throw new CoreException(NetErrors.WEBSOCKET_LOGIN_FAILED, "Login failed, accessToken {}, code {} message {} for theUrl {}", imClient.getCachedAccessToken(), code, message, finalTheUrl);
            });
        } catch (IOException e) {
            throw new CoreException(NetErrors.WEBSOCKET_LOGIN_FAILED, "Login url {} loginObj {} headers {} failed, {}", baseUrl, loginObj, headers, e.getMessage());
        }
        wsPort = data.getInteger("wsPort");
        wsPort = tapEngineUtils.getRealWsPort(wsPort, theUrl);
        sid = data.getString("token");
        String wsPath = data.getString("wsPath");
        wsPath = tapEngineUtils.getRealWsPath(wsPath, theUrl);
        String wsHost = data.getString("wsHost");
        String wsProtocol = data.getString("wsProtocol");
        if(wsProtocol != null) {
            protocol = wsProtocol;
        }
        if(wsHost != null) {
            host = wsHost;
        }
        path = wsPath;

        try {
            URL url = new URL(baseUrl);
            if(protocol == null) {
                protocol = (url.getProtocol().equals("https") ? "wss" : "ws");
            }
            if(host == null) {
                host = url.getHost();
            }
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

        if(protocol == null || wsPort == null || host == null || sid == null) {
            throw new CoreException(NetErrors.WEBSOCKET_LOGIN_FAILED, "Illegal parameters for wsPort " + wsPort + " host " + host + " server " + server + " sid " + sid + " protocol " + protocol);
        }
    }

    private String refreshAccessToken(String baseUrl) {
        String token = imClient.getToken();
        if(token == null) {
            throw new CoreException(NetErrors.TOKEN_NOT_FOUND, "Token not found");
        }
        int pos = baseUrl.indexOf("/api/") + "/api/".length();
        String newBaseUrl = baseUrl.substring(0, pos);

        JSONObject param = new JSONObject();
        param.put("accesscode", imClient.getToken());
        String newToken = null;
        JSONObject jsonObject = null;
        try {
            String theUrl = newBaseUrl + "users/generatetoken";
            TapEngineUtils tapEngineUtils = InstanceFactory.instance(TapEngineUtils.class);
            String jsonStr = JSON.toJSONString(param);
            theUrl = tapEngineUtils.signUrl("POST", theUrl, jsonStr);
            jsonObject = HttpUtils.post(theUrl, jsonStr, null);
            newToken = jsonObject.getString("id");
            String userId = jsonObject.getString("userId");
            if(userId != null) {
                imClient.setCachedCookie("user_id=" + userId);
            } else {
                imClient.setCachedCookie(null);
            }
        } catch (IOException e) {
            throw new CoreException(NetErrors.GENERATE_TOKEN_FAILED, e, "Generate access token failed, " + e.getMessage());
        }
        if(newToken == null) {
            throw new CoreException(NetErrors.GENERATE_TOKEN_FAILED, "Generate access token failed, token not found in result " + jsonObject);
        }
        return newToken;
    }

    private void connectWS(String protocol, final String host, final int port, String path) {
        if (!"ws".equalsIgnoreCase(protocol) && !"wss".equalsIgnoreCase(protocol)) {
            throw new CoreException(NetErrors.WEBSOCKET_PROTOCOL_ILLEGAL, "Only WS(S) is supported.");
        }

        final boolean ssl = "wss".equalsIgnoreCase(protocol);
        final SslContext sslCtx;
        if (ssl) {
            try {
                sslCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
            } catch (SSLException e) {
                throw new CoreException(NetErrors.WEBSOCKET_SSL_FAILED, "Build SSL failed, {}", e.getMessage());
            }
        } else {
            sslCtx = null;
        }

        URI uri = null;
        try {
            if(!isRemotePortAvailable(sslCtx, host, wsPort)) {
                TapLogger.info(TAG, "host {} wsPort {} is not available, will try others", host, wsPort);
                if("wss".equalsIgnoreCase(protocol)) {
                    if(isRemotePortAvailable(sslCtx, host, 443)) {
                        wsPort = 443;
                        TapLogger.info(TAG, "host {} wsPort {} is available, will use the new port", host, wsPort);
                    }
                } else {
                    if(isRemotePortAvailable(sslCtx, host, 80)) {
                        wsPort = 80;
                        TapLogger.info(TAG, "host {} wsPort {} is available, will use the new port", host, wsPort);
                    }
                }
            }

            String url = protocol + "://" + host + ":" + wsPort + "/" + path;
            TapEngineUtils tapEngineUtils = InstanceFactory.instance(TapEngineUtils.class);
            if(tapEngineUtils != null) {
                url = tapEngineUtils.signUrl("", url);
            }
            try {
                uri = new URI(url);
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
            if(uri == null)
                throw new CoreException(NetErrors.WEBSOCKET_URL_ILLEGAL, "uri illegal, " + protocol + "://" + host + ":" + port);

            TapLogger.info(TAG, "Connect uri {} wsPort {}", uri, wsPort);
            group = new NioEventLoopGroup(20);
            final WebSocketClientHandler handler = new WebSocketClientHandler(null, WebSocketClientHandshakerFactory
                    .newHandshaker(uri, WebSocketVersion.V13, null, false, new DefaultHttpHeaders(), 50 * 1024 * 1024));
            handler.pushChannel = this;

            Bootstrap b = new Bootstrap();
            b.option(ChannelOption.SO_KEEPALIVE,true)
                    .option(ChannelOption.TCP_NODELAY,true)
                    .option(ChannelOption.SO_RCVBUF, 1024 * 1024)
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

            channel = b.connect(uri.getHost(), wsPort).sync().channel();
            handler.handshakeFuture().sync();
        } catch (Throwable e) {
            TapLogger.error(TAG, "Connect uri {} wsPort {} failed, {}", uri, wsPort, e.getMessage());
            throw new CoreException(NetErrors.WEBSOCKET_CONNECT_FAILED, "Connect and handshake websocket failed, " + e.getMessage(), e);
        }
//        sendServer();
//        TapLogger.debug(TAG, "connectWS: "+"sendServer");
        Identity identity = new Identity();
        identity.setId("id");
        identity.setToken(sid);
        identity.setIdType(getImClient().getService());
        sendIdentity(identity);
        TapLogger.info(TAG, "connectWS: "+"sendIdentity"+identity);
    }
    private static boolean isRemotePortAvailable(SslContext sslCtx, final String host, int port) {
        EventLoopGroup group = new NioEventLoopGroup(1);
        Bootstrap b = new Bootstrap();
        b.option(ChannelOption.SO_KEEPALIVE,true)
                .option(ChannelOption.TCP_NODELAY,true)
                .option(ChannelOption.SO_RCVBUF, 1024 * 1024)
//                .option(ChannelOption.SO_BACKLOG,1024*1024*10)
                .group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        if (sslCtx != null) {
                            p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                        }
                    }
                });
        try {
            b.connect(host, port).sync().channel();
            return true;
        } catch (Throwable e) {
            TapLogger.error(TAG, "Try uri {} and port {} failed, {}", host, port, e.getMessage());
            return false;
        } finally {
            group.shutdownGracefully();
        }
    }
    private void sendIdentity(Identity data) {
        if(channel == null) {
            TapLogger.warn(TAG, "Channel not initialized before sending identity, {}", data);
            return;
        }

        send(data);
    }

    boolean isAlive() {
        return channel != null && isConnected;
    }

    public void ping() throws IOException {
        if(!isAlive()) return;
        if(pingFuture == null || pingFuture.isCancelled()) {
            Ping ping = new Ping();
            pingFuture = TimerEx.scheduleInSeconds(() -> {
                pingFuture = null;
                stop();
                TapLogger.info(TAG, "Stop channel because of ping timeout");
            }, 10);
            send(ping);
//            TapLogger.debug(TAG, "ping");
        }
    }

//    private void sendDataPrivate(Data data) throws IOException {
//        if(!isAlive()) return;
//
//        byte[] bytes = data.getData();
//        if(bytes == null) {
//            data.persistent();
//            bytes = data.getData();
//        }
//        if(bytes == null)
//            throw new IOException("Persistent data " + data.getClass() + " failed");
//
//
//        ByteBuf byteBuf = Unpooled.directBuffer(1 + 4 + bytes.length);
//        byteBuf.writeByte(data.getType());
//        byteBuf.writeInt(bytes.length);
//        byteBuf.writeBytes(bytes);
//
//        channel.writeAndFlush(new BinaryWebSocketFrame(byteBuf));
//    }

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

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        return DataMap.create().keyRegex(keyRegex)
                .kv("protocol", protocol)
                .kv("wsPort", wsPort)
                .kv("path", path)
                .kv("host", host)
//                .kv("key", key)
                .kv("baseUrl", baseUrl)
//                .kv("sid", sid)
                .kv("isConnected", isConnected)
                ;
    }
}
