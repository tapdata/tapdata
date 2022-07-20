package io.tapdata.connector.rocketmq;

import io.tapdata.common.MqService;
import io.tapdata.connector.rocketmq.config.RocketmqConfig;
import io.tapdata.constant.MqTestItem;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.remoting.RPCHook;

import java.util.List;
import java.util.function.BiConsumer;
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
        try {
            defaultMQProducer = new DefaultMQProducer(getRPCHook());
            defaultMQProducer.setNamesrvAddr(rocketmqConfig.getNameSrvAddr());
            defaultMQProducer.setProducerGroup(rocketmqConfig.getProducerGroup());
            defaultMQProducer.start();
            consumer.accept(new TestItem(MqTestItem.RABBIT_MQ_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null));
        } catch (Throwable t) {
            consumer.accept(new TestItem(MqTestItem.RABBIT_MQ_CONNECTION.getContent(), TestItem.RESULT_FAILED, t.getMessage()));
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

    @Override
    public int countTables() throws Throwable {
        return 0;
    }

    @Override
    public void loadTables(int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {

    }

    @Override
    public void produce(List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {

    }

    @Override
    public void consumeOne(TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {

    }

    @Override
    public void streamConsume(List<String> tableList, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {

    }
}
