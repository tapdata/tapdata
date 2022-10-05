package io.tapdata.observable.metric.aspect;

import com.tapdata.tm.commons.dag.Node;
import io.tapdata.entity.aspect.Aspect;

/**
 * @author Dexter
 */
public class ConnectionPingAspect extends Aspect {
    private static final String TAG = ConnectionPingAspect.class.getSimpleName();

    private Node<?> node;
    public ConnectionPingAspect node(Node<?> node) {
        this.node = node;
        return this;
    }

    private Long tcpPing;
    public ConnectionPingAspect tcpPing(Long tcpPing) {
        this.tcpPing = tcpPing;
        return this;
    }

    private Long connectPing;
    public ConnectionPingAspect connectPing(Long connectPing) {
        this.connectPing = connectPing;
        return this;
    }

    public Node<?> getNode() {
        return node;
    }

    public void setNode(Node<?> node) {
        this.node = node;
    }

    public Long getTcpPing() {
        return tcpPing;
    }

    public void setTcpPing(Long tcpPing) {
        this.tcpPing = tcpPing;
    }

    public Long getConnectPing() {
        return connectPing;
    }

    public void setConnectPing(Long connectPing) {
        this.connectPing = connectPing;
    }
}
