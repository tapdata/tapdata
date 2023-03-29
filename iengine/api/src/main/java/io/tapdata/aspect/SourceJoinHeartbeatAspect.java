package io.tapdata.aspect;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/3/29 15:36 Create
 */
public class SourceJoinHeartbeatAspect extends DataNodeAspect<SourceJoinHeartbeatAspect> {

    private boolean joinHeartbeat;

    public SourceJoinHeartbeatAspect joinHeartbeat(boolean joinHeartbeat) {
        this.joinHeartbeat = joinHeartbeat;
        return this;
    }

    public boolean getJoinHeartbeat() {
        return joinHeartbeat;
    }
}
