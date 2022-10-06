package io.tapdata.wsserver.channels.websocket.utils;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.tapdata.modules.api.net.data.Data;


public class NetUtils {
    public static boolean writeAndFlush(Channel channel, Data data) {
        if(channel != null && channel.isActive()) {
            if(data.getData() == null) {
                data.persistent();
            }
            ByteBuf byteBuf = Unpooled.directBuffer(1 + 1 + data.getData().length);
            try {
                // byteBuf.writeBytes(msgId.getBytes())
                // byteBuf.writeShort(Integer.parseInt(msgId))
                byteBuf.writeByte(data.getType());
                byteBuf.writeByte(Data.ENCODE_JAVA_CUSTOM_SERIALIZER);
                if(data.getData().length > 0)
                    byteBuf.writeBytes(data.getData());
                channel.writeAndFlush(new BinaryWebSocketFrame(byteBuf));
                return true;
            } catch(Throwable t) {
                t.printStackTrace();
                byteBuf.release();
            }
        }
        return false;
    }

    public static boolean writeAndFlush(Channel channel, byte type) {
        if(channel != null && channel.isActive()) {
            ByteBuf byteBuf = Unpooled.directBuffer(1);
            try {
                // byteBuf.writeBytes(msgId.getBytes())
                // byteBuf.writeShort(Integer.parseInt(msgId))
                byteBuf.writeByte(type);
                channel.writeAndFlush(new BinaryWebSocketFrame(byteBuf));
                return true;
            } catch(Throwable t) {
                t.printStackTrace();
                byteBuf.release();
            }
        }
        return false;
    }
}
