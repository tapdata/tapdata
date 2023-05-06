package io.tapdata.wsclient.modules.imclient.impls.websocket;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.modules.api.net.data.Data;
import io.tapdata.modules.api.net.data.OutgoingData;
import io.tapdata.modules.api.net.data.Ping;
import io.tapdata.modules.api.net.data.Result;
import io.tapdata.wsclient.modules.imclient.impls.data.DataVersioning;
import io.tapdata.wsclient.utils.EventManager;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketHandshakeException;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.commons.lang3.exception.ExceptionUtils;

public class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
    private static final String TAG = WebSocketClientHandler.class.getSimpleName();
    private Channel outboundChannel;
    private ChannelHandlerContext channel;
    private static ChannelGroup clients = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final WebSocketClientHandshaker handshaker;
    private ChannelPromise handshakeFuture;
    int ERROR_CHANNEL_KICKED_BY_DEVICE = 6014;
    int ERROR_CHANNEL_KICKED_BY_CONCURRENT = 6015;
    int ERROR_LOGIN_FAILED_DEVICE_TOKEN_CHANGED = 6016;
    int ERROR_EXCEED_USER_CHANNEL_CAPACITY = 6017;
    int ERROR_CHANNEL_BYE_BYE = 6025;
    WebsocketPushChannel pushChannel;

    private EventManager eventManager;

    public WebSocketClientHandler(ChannelHandlerContext channel, WebSocketClientHandshaker handshaker) {
        this.handshaker = handshaker;
        this.channel = channel;
        eventManager = EventManager.getInstance();
    }

    public ChannelFuture handshakeFuture() {
        return handshakeFuture;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        TapLogger.debug(TAG, "handlerAdded, " + ctx.name());
        handshakeFuture = ctx.newPromise();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
//        TapLogger.debug(TAG, "channelActive, " + ctx.name());
        handshaker.handshake(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
//        TapLogger.debug(TAG, "channelInactive, " + ctx.name());
        pushChannel.isConnected = false;
        eventManager.sendEvent(pushChannel.getImClient().getPrefix() + ".status", new ChannelStatus(pushChannel, ChannelStatus.STATUS_DISCONNECTED));
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
//        System.out.println("channelRead0");
        Channel ch = ctx.channel();
        if (!handshaker.isHandshakeComplete()) {
            try {
                handshaker.finishHandshake(ch, (FullHttpResponse) msg);
//                System.out.println("WebSocket Client connected!");
                handshakeFuture.setSuccess();
            } catch (WebSocketHandshakeException e) {
//                System.out.println("WebSocket Client failed to connect");
                handshakeFuture.setFailure(e);
            }
            return;
        }

        if (msg instanceof FullHttpResponse) {
            FullHttpResponse response = (FullHttpResponse) msg;
            throw new IllegalStateException("Unexpected FullHttpResponse (getStatus=" + response.getStatus()
                    + ", content=" + response.content().toString(CharsetUtil.UTF_8) + ')');
        }

        WebSocketFrame frame = (WebSocketFrame) msg;
        if(frame instanceof BinaryWebSocketFrame) {
//            TapLogger.debug(TAG, "channel read");
            try {
                BinaryWebSocketFrame binaryWebSocketFrame = (BinaryWebSocketFrame) frame;
                ByteBuf byteBuf = binaryWebSocketFrame.content();
                byte type = byteBuf.readByte();
                byte encode = Data.ENCODE_JAVA_CUSTOM_SERIALIZER;
                byte[] bytes = null;
                if(byteBuf.readableBytes() > 0) {
                    encode = byteBuf.readByte();
                    bytes = new byte[byteBuf.readableBytes()];
                    byteBuf.readBytes(bytes);
                }
                Data data = DataVersioning.get(encode, type);
                if(bytes != null) {
                    data.setData(bytes);
                    data.resurrect();
                }

                if(pushChannel != null && pushChannel.getImClient() != null) {
                    String prefix = pushChannel.getImClient().getPrefix();
                    //Any data received will cancel the ping timer.
                    if(pushChannel.pingFuture != null) {
//                    TapLogger.debug(TAG, "ping timeout canceled");
                        pushChannel.pingFuture.cancel(true);
                        pushChannel.pingFuture = null;
                    }
                    switch (type) {
                        case Ping.TYPE:
//                        TapLogger.debug(TAG, "pong");
                            break;
                        case Result.TYPE:
                            Result result = (Result) data;
                            if(result.getCode() == 1 && !pushChannel.isConnected) {
                                pushChannel.isConnected = true;
                                TapLogger.debug(TAG, "PushChannel connected");
                                eventManager.sendEvent(prefix + ".status", new ChannelStatus(pushChannel, ChannelStatus.STATUS_CONNECTED));
                            } else if(result.getCode() == 11) {
                                eventManager.sendEvent(prefix + ".status", new ChannelStatus(pushChannel, ChannelStatus.STATUS_OFFLINEMESSAGECONSUMED));
                            } else if(
                                            result.getCode() == ERROR_CHANNEL_KICKED_BY_DEVICE ||
                                            result.getCode() == ERROR_CHANNEL_KICKED_BY_CONCURRENT ||
                                            result.getCode() == ERROR_LOGIN_FAILED_DEVICE_TOKEN_CHANGED ||
                                            result.getCode() == ERROR_EXCEED_USER_CHANNEL_CAPACITY
                            ) { //kicked
                                TapLogger.info(TAG, "PushChannel kicked, code {}", result.getCode());
                                eventManager.sendEvent(pushChannel.getImClient().getPrefix() + ".status", new ChannelStatus(pushChannel, ChannelStatus.STATUS_KICKED));
                            } else if(result.getCode() == ERROR_CHANNEL_BYE_BYE) {
                                TapLogger.info(TAG, "PushChannel bye, code {}", result.getCode());
                                eventManager.sendEvent(pushChannel.getImClient().getPrefix() + ".status", new ChannelStatus(pushChannel, ChannelStatus.STATUS_BYE));
                            } else {
                                TapLogger.debug(TAG, "PushChannel receive result " + result);
                                eventManager.sendEvent(prefix + ".result", result);
                            }
                            break;
                        default:
                            eventManager.sendEvent(prefix + "." + data.getClass().getSimpleName(), data);
                            eventManager.sendEvent(prefix + "." + data.getClass().getSimpleName() + "." + data.getContentType(), data);
                            break;
//                        case HailPack.TYPE_OUT_OUTGOINGDATA:
//                            OutgoingData outgoingData = (OutgoingData) data;
//                            eventManager.sendEvent(prefix + ".data", outgoingData);
//                            eventManager.sendEvent(prefix + ".data." + outgoingData.getContentType(), outgoingData);
//                            break;
                    }
                }
            } catch (Throwable throwable) {
                TapLogger.error(TAG, "Receive message occurred error {}", ExceptionUtils.getStackTrace(throwable));
            }
        } else if (frame instanceof TextWebSocketFrame) {
            TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
            response(ctx, frame);

            channel.writeAndFlush(textFrame.text());
            TapLogger.warn(TAG, "WebSocket Client received message: " + textFrame.text());
        } else if (frame instanceof PongWebSocketFrame) {
            TapLogger.warn(TAG, "WebSocket Client received pong");
        } else if (frame instanceof CloseWebSocketFrame) {
            TapLogger.warn(TAG, "WebSocket Client received closing");
            ch.close();
        }
    }

    private void response(ChannelHandlerContext ctx, final WebSocketFrame msg) {
        // 获取客户端传输过来的消息
        String content = msg.toString();
        clients.writeAndFlush(new TextWebSocketFrame("[服务器收到相应]接受萨达到消息, 消息为：" + content));

        final Channel inboundChannel = ctx.channel();

        Bootstrap b = new Bootstrap();
        b.group(inboundChannel.eventLoop()).channel(ctx.channel().getClass())
                .handler(new SocketHandlerInitializer(inboundChannel));

        ChannelFuture f = b.connect("127.0.0.1", 5688);
        outboundChannel = f.channel();
        msg.retain();

        ChannelFuture channelFuture = f.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    System.out.println("isSuccess:true");
                    outboundChannel.writeAndFlush("2222222222");
                } else {
                    System.out.println("isSuccess：false");
                    inboundChannel.close();
                }
            }
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        TapLogger.error(TAG, "exceptionCaught, " + ctx.name() + " cause " + cause.getMessage(), ExceptionUtils.getStackTrace(cause));
        cause.printStackTrace();
        if (!handshakeFuture.isDone()) {
            handshakeFuture.setFailure(cause);
        }
        ctx.close();
    }

}
