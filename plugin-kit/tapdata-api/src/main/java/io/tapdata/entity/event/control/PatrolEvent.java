package io.tapdata.entity.event.control;

import io.tapdata.entity.event.TapEvent;

import java.util.HashMap;
import java.util.Map;

public class PatrolEvent extends ControlEvent {
    public static final int TYPE = 400;

    public static final int STATE_ENTER = 1;
    public static final int STATE_LEAVE = 10;
//    public static final int STATE_END = 100;

    private Map<String, Integer> stateMap = new HashMap<>();

    private PatrolListener patrolListener;

    public PatrolEvent() {
        super(TYPE);
    }

    public PatrolEvent(int type) {
        super(type);
    }

    public PatrolEvent patrolListener(PatrolListener patrolListener) {
        this.patrolListener = patrolListener;
        return this;
    }

    public boolean applyState(String nodeId, int state) {
        Integer theState = stateMap.get(nodeId);
        if(theState == null) {
            stateMap.put(nodeId, state);
            return true;
        } else {
            if(theState < state) {
                stateMap.put(nodeId, state);
                return true;
            }
        }
        return false;
    }

    @Override
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if(tapEvent instanceof PatrolEvent) {
            PatrolEvent patrolEvent = (PatrolEvent) tapEvent;
            //use the same listener to trace.
            patrolEvent.patrolListener = patrolListener;
            patrolEvent.stateMap = stateMap;
        }
    }

    public PatrolListener getPatrolListener() {
        return patrolListener;
    }

    public void setPatrolListener(PatrolListener patrolListener) {
        this.patrolListener = patrolListener;
    }
}
