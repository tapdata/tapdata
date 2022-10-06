package io.tapdata.wsclient.modules.imclient.impls.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;

public class SocketHandlerInitializer extends ChannelInboundHandlerAdapter {
    Channel channel;
    public SocketHandlerInitializer(Channel channel) {
        this.channel = channel;
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        final WebSocketFrame frame = (WebSocketFrame) msg;
//        if (frame instanceof BinaryWebSocketFrame) {
//            //junior
//            ByteBuf content = ((WebSocketFrame) msg).content();
//            if (!frame.isFinalFragment()) {
//                byte[] bytes = new byte[content.readableBytes()];
//                for (int i = 0; i < content.readableBytes(); i++) {
//                    bytes[i] = content.getByte(i);
//                }
//                _binData.add(bytes);
//                return;
//            }
//            byte[] bytes = new byte[content.readableBytes()];
//            for (int i = 0; i < content.readableBytes(); i++) {
//                bytes[i] = content.getByte(i);
//            }
//            _client.onBinaryMessage(ByteBuffer.wrap(bytes));
//
//            return;
//        } else if(frame instanceof ContinuationWebSocketFrame) {
//            ByteBuf content = ((WebSocketFrame) msg).content();
//            byte[] bytes = new byte[content.readableBytes()];
//            for (int i = 0; i < content.readableBytes(); i++) {
//                bytes[i] = content.getByte(i);
//            }
//            _binData.add(bytes);
//            if (!frame.isFinalFragment()) {
//                return;
//            }
//            byte[] allBytes = new byte[0];
//            for (byte[] _bytes : _binData) {
//                allBytes = ArrayUtils.addAll(allBytes, _bytes);
//            }
//            _binData.clear();
//            _client.onBinaryMessage(ByteBuffer.wrap(allBytes));
//        }
    }

    @Override
    public void handlerAdded(ChannelHandlerContext channelHandlerContext) throws Exception {

    }

    @Override
    public void handlerRemoved(ChannelHandlerContext channelHandlerContext) throws Exception {

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable throwable) throws Exception {

    }
}
