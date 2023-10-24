package io.tapdata.wsserver.channels.websocket.modules;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.MainMethod;
import io.tapdata.wsserver.channels.websocket.event.ChannelActiveEvent;
import io.tapdata.wsserver.channels.websocket.event.IdentityReceivedEvent;
import io.tapdata.wsserver.eventbus.EventBusHolder;
import io.tapdata.pdk.core.executor.ExecutorsManager;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@MainMethod(value = "main", order = 1000)
@Bean
public class DummyChannelDetectionModule {
    public void main() {
        EventBusHolder.getEventBus().register(this);
    }


    public final String ATTR_EXPIRE_TIMER = "DUMMY_ATTR_EXPIRE_TIMER";
    private long expireSeconds = 8; //TODO should be 8, only for test.

    @Subscribe
    @AllowConcurrentEvents
    public void channelCreated(ChannelActiveEvent channelActiveEvent) {
        Channel channel = channelActiveEvent.getCtx().channel();
        if (channel != null) {
            ScheduledFuture<?> future = ExecutorsManager.getInstance().getScheduledExecutorService().schedule(() -> {
                Channel channel1 = channelActiveEvent.getCtx().channel();
                if (channel1 != null && channel1.isActive()) {
                    try {
                        channel1.close();
                    } catch (Throwable ignored) {
                    }
                }
            }, expireSeconds, TimeUnit.SECONDS);
//            Attribute<ScheduledFuture> attribute = channel.attr(AttributeKey<ScheduledFuture>.valueOf(ATTR_EXPIRE_TIMER));
            Attribute<ScheduledFuture<?>> attribute = channel.attr(AttributeKey.valueOf(ATTR_EXPIRE_TIMER));
            attribute.set(future);
        }
    }

    @Subscribe
    @AllowConcurrentEvents
    public void dataReceived(IdentityReceivedEvent dataReceivedEvent) {
        Channel channel = dataReceivedEvent.getCtx().channel();
        if (channel != null) {
            Attribute<ScheduledFuture<?>> attribute = channel.attr(AttributeKey.valueOf(ATTR_EXPIRE_TIMER));
            ScheduledFuture<?> future = attribute.getAndSet(null);
            if (future != null) {
                future.cancel(false);
            }
        }
    }

    public long getExpireSeconds() {
        return expireSeconds;
    }

    public void setExpireSeconds(long expireSeconds) {
        this.expireSeconds = expireSeconds;
    }
}
