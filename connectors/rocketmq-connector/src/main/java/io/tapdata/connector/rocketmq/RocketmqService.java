package io.tapdata.connector.rocketmq;

import io.tapdata.common.MqService;
import io.tapdata.connector.rocketmq.config.RocketmqConfig;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TestItem;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.remoting.RPCHook;

import java.util.function.Consumer;

public class RocketmqService implements MqService {

    private static final String TAG = RocketmqService.class.getSimpleName();
    private final RocketmqConfig rocketmqConfig;
    private DefaultMQProducer defaultMQProducer;

    public RocketmqService(RocketmqConfig rocketmqConfig) {
        this.rocketmqConfig = rocketmqConfig;
    }

    @Override
    public void testConnection(Consumer<TestItem> consumer) {
        defaultMQProducer = new DefaultMQProducer(getRPCHook());
        defaultMQProducer.setNamesrvAddr(rocketmqConfig.getNameSrvAddr());
        defaultMQProducer.setProducerGroup(rocketmqConfig.getProducerGroup());
        try {
            defaultMQProducer.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void init() {

    }

    public RPCHook getRPCHook() {
        if (EmptyKit.isNotBlank(rocketmqConfig.getMqUsername()) && EmptyKit.isNotBlank(rocketmqConfig.getMqPassword())) {
            return new AclClientRPCHook(new SessionCredentials(rocketmqConfig.getMqUsername(), rocketmqConfig.getMqPassword()));
        }
        return null;
    }

    @Override
    public void close() {
        if (EmptyKit.isNotNull(defaultMQProducer)) {
            defaultMQProducer.shutdown();
        }
    }
}
