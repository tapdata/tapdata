package io.tapdata.wsserver.channels.websocket.event;


import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.tapdata.modules.api.net.data.Ping;
import io.tapdata.modules.api.net.data.Result;
import io.tapdata.wsserver.channels.websocket.utils.NetUtils;

public class NettyEvent<T extends NettyEvent<?>> {
    private ChannelHandlerContext ctx;

    public T ctx(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        return (T) this;
    }

    public ChannelHandlerContext getCtx() {
        return ctx;
    }

    public void setCtx(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    public boolean ping(Channel channel) {
        return NetUtils.writeAndFlush(channel, Ping.TYPE);
    }

    public boolean sendResult(Result result) {
        Channel channel = ctx.channel();
        return this.sendResult(channel, result);
    }

    public boolean sendResult(Channel channel, Result result) {
        return NetUtils.writeAndFlush(channel, result);
    }
    public void closeChannel(Channel channel, String forId, int code) {
        closeChannel(channel, forId, code, null);
    }
    public void closeChannel(Channel channel, String forId, int code, String description) {
        if(channel != null && channel.isActive()) {
            try {
                sendResult(channel, Result.create().forId(forId).code(code).description(description).time(System.currentTimeMillis()));
            } catch(Throwable ignored) {} finally {
                channel.close();
            }
        }
    }
    public void closeChannel(String forId, int code) {
        closeChannel(forId, code, null);
    }
    public void closeChannel(String forId, int code, String description) {
        Channel channel = ctx.channel();
        closeChannel(channel, forId, code, description);
    }
}
