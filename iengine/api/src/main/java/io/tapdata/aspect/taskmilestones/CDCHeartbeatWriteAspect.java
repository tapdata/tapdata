package io.tapdata.aspect.taskmilestones;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.aspect.DataNodeAspect;

import java.util.List;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/3/21 14:08 Create
 */
public class CDCHeartbeatWriteAspect extends DataNodeAspect<CDCHeartbeatWriteAspect> {

    private List<TapdataEvent> tapdataEvents;

    public List<TapdataEvent> getTapdataEvents() {
        return tapdataEvents;
    }

    public CDCHeartbeatWriteAspect tapdataEvents(List<TapdataEvent> tapdataEvents) {
        this.tapdataEvents = tapdataEvents;
        return this;
    }
}
