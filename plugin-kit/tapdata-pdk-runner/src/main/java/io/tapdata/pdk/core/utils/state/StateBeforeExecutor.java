package io.tapdata.pdk.core.utils.state;

public interface StateBeforeExecutor<K, T> {
    void execute(T t, StateMachine<K, T> stateMachine) throws Throwable;
}
