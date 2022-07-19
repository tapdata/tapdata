package io.tapdata.connector.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.tapdata.common.MqService;
import io.tapdata.connector.rabbitmq.config.RabbitmqConfig;
import io.tapdata.constant.MqTestItem;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TestItem;

import java.io.IOException;
import java.util.function.Consumer;

public class RabbitmqService implements MqService {

    private static final String TAG = RabbitmqService.class.getSimpleName();
    private final RabbitmqConfig rabbitmqConfig;
    private Connection rabbitmqConnection;

    public RabbitmqService(RabbitmqConfig rabbitmqConfig) {
        this.rabbitmqConfig = rabbitmqConfig;
    }

    @Override
    public void testConnection(Consumer<TestItem> consumer) {
        try {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(rabbitmqConfig.getMqHost());
            connectionFactory.setPort(rabbitmqConfig.getMqPort());
            connectionFactory.setUsername(rabbitmqConfig.getMqUsername());
            connectionFactory.setPassword(rabbitmqConfig.getMqPassword());
            connectionFactory.setVirtualHost(rabbitmqConfig.getVirtualHost());
            rabbitmqConnection = connectionFactory.newConnection();
            consumer.accept(new TestItem(MqTestItem.RABBIT_MQ_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null));
        } catch (Throwable t) {
            consumer.accept(new TestItem(MqTestItem.RABBIT_MQ_CONNECTION.getContent(), TestItem.RESULT_FAILED, t.getMessage()));
        } finally {
            close();
        }
    }

    @Override
    public void init() {

    }

    @Override
    public void close() {
        try {
            if (EmptyKit.isNotNull(rabbitmqConnection)) {
                rabbitmqConnection.close();
            }
        } catch (IOException e) {
            TapLogger.error(TAG, "close connection error", e);
        }
    }
}
