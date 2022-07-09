package io.tapdata.connector.activemq;

import io.tapdata.common.MqConfig;
import io.tapdata.common.MqService;
import io.tapdata.connector.activemq.config.ActivemqConfig;

public class ActivemqService extends MqService {

    public ActivemqService(MqConfig mqConfig) {
        super(mqConfig);
    }

    @Override
    public void testConnection() {
        ActivemqConfig activemqConfig = (ActivemqConfig) mqConfig;
    }
}
