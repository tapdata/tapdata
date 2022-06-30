package io.tapdata.pdk.core.utils.state;

public interface StateErrorOccurredExecutor<K, T> {
    void onError(Throwable throwable, K fromState, K toState, T t, StateMachine<K, T> stateMachine);
}
