package io.tapdata.connector.rabbitmq;

import com.rabbitmq.client.AMQP;
import io.tapdata.common.message.AbstractMqMessage;

public class RabbitmqMessage extends AbstractMqMessage {

    private String routeKey;
    private AMQP.BasicProperties props;
    private byte[] body;

    public String getRouteKey() {
        return routeKey;
    }

    public void setRouteKey(String routeKey) {
        this.routeKey = routeKey;
    }

    public AMQP.BasicProperties getProps() {
        return props;
    }

    public void setProps(AMQP.BasicProperties props) {
        this.props = props;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
