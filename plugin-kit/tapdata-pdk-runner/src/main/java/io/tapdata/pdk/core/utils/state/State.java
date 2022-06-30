package io.tapdata.pdk.core.utils.state;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class State<K, T> {
    private K state;
    private StateExecutor<K, T> stateExecutor, leaveStateExecutor;
    private Set<K> gotoStates;

    public State(StateExecutor<K, T> stateExecutor) {
        this.stateExecutor = stateExecutor;
    }

    public State<K, T> leaveState(StateExecutor<K, T> leaveStateExecutor) {
        this.leaveStateExecutor = leaveStateExecutor;
        return this;
    }

    public State<K, T> nextStates(K... states) {
        if(states != null && states.length > 0) {
            gotoStates = Collections.synchronizedSet(new HashSet<>(Arrays.asList(states)));
        } else {
            gotoStates = null;
        }
        return this;
    }

    public K getState() {
        return state;
    }

    public void setState(K state) {
        this.state = state;
    }

    public StateExecutor<K, T> getStateExecutor() {
        return stateExecutor;
    }

    public void setStateExecutor(StateExecutor<K, T> stateExecutor) {
        this.stateExecutor = stateExecutor;
    }

    public StateExecutor<K, T> getLeaveStateExecutor() {
        return leaveStateExecutor;
    }

    public void setLeaveStateExecutor(StateExecutor<K, T> leaveStateExecutor) {
        this.leaveStateExecutor = leaveStateExecutor;
    }

    public Set<K> getGotoStates() {
        return gotoStates;
    }

    public void setGotoStates(Set<K> gotoStates) {
        this.gotoStates = gotoStates;
    }
}
