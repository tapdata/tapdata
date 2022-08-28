package com.dobybros.tccore.modules.imclient.impls.websocket;

import com.dobybros.tccore.modules.imclient.impls.data.Data;
import com.dobybros.tccore.modules.imclient.impls.data.HailPack;
import com.dobybros.tccore.modules.imclient.impls.data.OutgoingData;
import com.dobybros.tccore.modules.imclient.impls.data.OutgoingMessage;
import com.dobybros.tccore.modules.imclient.impls.data.Result;
import com.dobybros.tccore.utils.EventManager;
import com.dobybros.tccore.utils.LoggerEx;

import java.time.LocalDateTime;

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
import ws.SocketHandlerInitializer;

public class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
    private static final String TAG = WebSocketClientHandler.class.getSimpleName();
    private Channel outboundChannel;
    private ChannelHandlerContext channel;
    private static ChannelGroup clients = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final WebSocketClientHandshaker handshaker;
    private ChannelPromise handshakeFuture;

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
        LoggerEx.info(TAG, "handlerAdded, " + ctx.name());
        handshakeFuture = ctx.newPromise();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        LoggerEx.info(TAG, "channelActive, " + ctx.name());
        handshaker.handshake(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        LoggerEx.info(TAG, "channelInactive, " + ctx.name());
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
            LoggerEx.info(TAG, "channel read");
            BinaryWebSocketFrame binaryWebSocketFrame = (BinaryWebSocketFrame) frame;
            ByteBuf byteBuf = binaryWebSocketFrame.content();
            byte type = byteBuf.readByte();
            int length = byteBuf.readInt();
            byte[] bytes = new byte[length];
            byteBuf.readBytes(bytes);
            Data data = null;
            switch (type) {
                case HailPack.TYPE_OUT_RESULT:
                    data = new Result();
                    break;
                case HailPack.TYPE_OUT_OUTGOINGDATA:
                    data = new OutgoingData();
                    break;
                default:
                    LoggerEx.error("illegal message type " + type + " received in websocket channel, ignored");
                    break;
            }
            if(data != null) {
                data.setData(bytes);
                data.setEncode(Data.ENCODE_PB);
                data.setEncodeVersion(WebsocketPushChannel.encodeVersion);
                data.resurrect();

                if(pushChannel != null && pushChannel.getImClient() != null) {
                    String prefix = pushChannel.getImClient().getPrefix();
                    switch (type) {
                        case HailPack.TYPE_OUT_RESULT:
                            Result result = (Result) data;

                            String forId = result.getForId();
                            if(forId != null && forId.equals("ping") && pushChannel.pingFuture != null) {
                                LoggerEx.info(TAG, "pong");
                                if(pushChannel.pingFuture != null) {
                                    pushChannel.pingFuture.cancel(true);
                                    pushChannel.pingFuture = null;
                                }
                            }

                            if(result.getCode() == 1 && !pushChannel.isConnected) {
                                pushChannel.isConnected = true;
                                LoggerEx.info(TAG, "PushChannel connected");
                                eventManager.sendEvent(prefix + ".status", new ChannelStatus(pushChannel, ChannelStatus.STATUS_CONNECTED));
                            } else if(result.getCode() == 11) {
                                eventManager.sendEvent(prefix + ".status", new ChannelStatus(pushChannel, ChannelStatus.STATUS_OFFLINEMESSAGECONSUMED));
                            } else if(result.getCode() == 1075) { //kicked
                                LoggerEx.info(TAG, "PushChannel kicked");
                                eventManager.sendEvent(pushChannel.getImClient().getPrefix() + ".status", new ChannelStatus(pushChannel, ChannelStatus.STATUS_KICKED));
                            } else if(result.getCode() == 1094) {
                                LoggerEx.info(TAG, "PushChannel byed");
                                eventManager.sendEvent(pushChannel.getImClient().getPrefix() + ".status", new ChannelStatus(pushChannel, ChannelStatus.STATUS_BYE));
                            } else {
                                LoggerEx.info(TAG, "PushChannel receive result " + result);
                                eventManager.sendEvent(prefix + ".result", result);
                            }
                            break;
                        case HailPack.TYPE_OUT_OUTGOINGDATA:
                            OutgoingData outgoingData = (OutgoingData) data;
                            eventManager.sendEvent(prefix + ".data", outgoingData);
                            eventManager.sendEvent(prefix + ".data." + outgoingData.getContentType(), outgoingData);
                            break;
                    }
                }
            }
        } else if (frame instanceof TextWebSocketFrame) {
            TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
            response(ctx, frame);
            
            channel.writeAndFlush(textFrame.text());
            System.out.println("WebSocket Client received message: " + textFrame.text());
        } else if (frame instanceof PongWebSocketFrame) {
            System.out.println("WebSocket Client received pong");
        } else if (frame instanceof CloseWebSocketFrame) {
            System.out.println("WebSocket Client received closing");
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
        LoggerEx.error(TAG, "exceptionCaught, " + ctx.name() + " cause " + cause.getMessage(), cause);
        cause.printStackTrace();
        if (!handshakeFuture.isDone()) {
            handshakeFuture.setFailure(cause);
        }
        ctx.close();
    }

}