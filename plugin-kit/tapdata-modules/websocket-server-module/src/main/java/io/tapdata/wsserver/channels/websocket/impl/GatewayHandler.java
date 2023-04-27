package io.tapdata.wsserver.channels.websocket.impl;

import com.google.common.eventbus.EventBus;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.timeout.IdleStateEvent;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.modules.api.net.data.*;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.wsserver.channels.error.WSCoreException;
import io.tapdata.wsserver.channels.error.WSErrors;
import io.tapdata.wsserver.channels.websocket.event.*;
import io.tapdata.wsserver.channels.websocket.utils.NetUtils;
import io.tapdata.wsserver.eventbus.EventBusHolder;
import org.apache.commons.lang3.exception.ExceptionUtils;

public class GatewayHandler extends AbstractWebSocketServerHandler {
    private final static String TAG = GatewayHandler.class.getSimpleName();

    private final EventBus eventBus = EventBusHolder.getEventBus();

    public GatewayHandler(boolean ssl) {
        super(ssl);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
//        LoggerEx.info(TAG, "channelActive $ctx")
//        ctx.channel().attr(AttributeKey.valueOf(""))
//        sessionManager.create(ctx.channel())
        eventBus.post(new ChannelActiveEvent().ctx(ctx));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
//        LoggerEx.info(TAG, "channelInactive $ctx")
//        sessionManager.remove(ctx.channel())
        eventBus.post(new ChannelInActiveEvent().ctx(ctx));
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
//        LoggerEx.info(TAG, "userEventTriggered $ctx $evt")
        if (evt instanceof IdleStateEvent) {
//            logger.debug("channel {} idle {}", ctx.channel(), ((IdleStateEvent) evt).state().name())
//            ctx.close()
        }
    }
    public boolean sendResult(ChannelHandlerContext ctx, Result result) {
        return NetUtils.writeAndFlush(ctx.channel(), result);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        TapLogger.debug(TAG, "exceptionCaught {} {}", ctx, ExceptionUtils.getStackTrace(cause));

//        Channel channel = ctx.channel();
//        if(channel != null && channel.isActive()) {
//            String forId = null;
//            Integer code = null;
//            if(cause instanceof WSCoreException) {
//                WSCoreException wsCoreException = (WSCoreException) cause;
//                forId = wsCoreException.getForId();
//                code = wsCoreException.getCode();
//            } else if(cause instanceof CoreException) {
//                CoreException coreException = (CoreException) cause;
//                code = coreException.getCode();
//            } else {
//                code = WSErrors.ERROR_UNKNOWN;
//            }
//
//            try {
//                sendResult(ctx, Result.create().forId(forId).code(code).description(cause.getMessage()).time(System.currentTimeMillis()));
//            } catch(Throwable ignored) {} finally {
//                try { channel.close(); } catch(Throwable ignored) {}
//            }
//        }
    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, BinaryWebSocketFrame webSocketFrame) {
//        TapLogger.debug(TAG, "messageReceived $ctx $webSocketFrame");
        byte[] body = null;

        ByteBuf byteBuf = webSocketFrame.content();
        byte type = byteBuf.readByte();
        checkType(type, ctx);

        byte encode = Data.ENCODE_JAVA_CUSTOM_SERIALIZER;
        if(byteBuf.readableBytes() > 0) {
            encode = byteBuf.readByte();
            int readableBytes = byteBuf.readableBytes();
//        if(readableBytes > 32768)
//            throw new IllegalArgumentException("Received bytes is bigger than 32768, ignore...")
            body = new byte[readableBytes];
            byteBuf.readBytes(body);
        }

        switch (encode) {
            case Data.ENCODE_JAVA_CUSTOM_SERIALIZER:
            case Data.ENCODE_PB:
            case Data.ENCODE_JSON:
                break;
            default:
                throw new CoreException(NetErrors.ILLEGAL_ENCODE, "Illegal encode {}", encode);
        }


        switch (type) {
            case Identity.TYPE:
                eventBus.post(new IdentityReceivedEvent().identity(new Identity(body, encode)).ctx(ctx));
                break;
            case IncomingData.TYPE:
                eventBus.post(new IncomingDataReceivedEvent().incomingData(new IncomingData(body, encode)).ctx(ctx));
                break;
            case Ping.TYPE:
                eventBus.post(new PingReceivedEvent().ping(new Ping()).ctx(ctx));
                break;
            case IncomingMessage.TYPE:
                eventBus.post(new IncomingMessageReceivedEvent().incomingMessage(new IncomingMessage(body, encode)).ctx(ctx));
                break;
            case IncomingInvocation.TYPE:
                eventBus.post(new IncomingInvocationReceivedEvent().incomingInvocation(new IncomingInvocation(body, encode)).ctx(ctx));
                break;
            default:
                TapLogger.error(TAG, "Unexpected type received {}, length {}. Ignored...", type, body != null ? body.length : 0);
                break;
        }
    }

    private static void checkType(byte type, ChannelHandlerContext ctx) {
        switch (type) {
            case Identity.TYPE:
            case IncomingData.TYPE:
            case Ping.TYPE:
            case IncomingInvocation.TYPE:
            case IncomingMessage.TYPE:
                break;
            default:
                throw new IllegalArgumentException("Illegal type $type received from ctx ${ctx}");
        }
    }
}