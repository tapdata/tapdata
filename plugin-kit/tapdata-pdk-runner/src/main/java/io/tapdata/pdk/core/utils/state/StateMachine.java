package io.tapdata.pdk.core.utils.state;

import io.tapdata.entity.logger.TapLogger;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

public class StateMachine<K, T> {
    private static final String TAG = StateMachine.class.getSimpleName();
    // 声明这个状态机的实例化类
    T t;
    // 当前状态
    private K currentState;
    // 状态所对应的处理器
    private ConcurrentHashMap<K, State<K, T>> stateMap = new ConcurrentHashMap<>();
    private boolean stateMachineStated = false;
    // 发生错误时的处理器（eg：改变state时失败）
    private StateErrorOccurredExecutor<K, T> stateErrorOccurredExecutor;
    // state变化后的监听器
    private CopyOnWriteArrayList<StateListener<K, T>> stateListeners;
    private final String name;
    private ExecutorService executorService;

    public StateMachine(String name, K initialState, T t) {
        this.currentState = initialState;
        this.t = t;
        this.name = name;
    }

    public void enableAsync(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public String toString() {
        return "StateMachine: " + this.name + "@state " + this.currentState;
    }

    public StateMachine<K, T> addStateListener(StateListener<K, T> stateListener) {
        if(stateListeners == null) {
            synchronized (this) {
                if(stateListeners == null) {
                    stateListeners = new CopyOnWriteArrayList<>();
                }
            }
        }
        if(!stateListeners.contains(stateListener)) {
            stateListeners.add(stateListener);
        }
        return this;
    }

    public boolean removeStateListener(StateListener<K, T> stateListener) {
        if(stateListeners != null) {
            return stateListeners.remove(stateListener);
        }
        return false;
    }

    public State<K, T> execute() {
        return new State<K, T>(null);
    }

    // 到达当前状态的回调
    public State<K, T> execute(StateExecutor<K, T> stateExecutor) {
        return new State<K, T>(stateExecutor);
    }

    public StateExecutor<K, T> newExecutor(StateExecutor<K, T> stateExecutor) {
        return stateExecutor;
    }

    public synchronized void restart() {
        stateMachineStated = false;
        currentState = null;
//        changeState(null, "restarted");
    }

    public synchronized void reset() {
        restart();
        stateMap.clear();
    }

    public StateMachine<K, T> configState(K state, State<K, T> stateObj) {
        if(stateMachineStated)
            throw new IllegalStateException(name + ": StateMachine is already started, can not config states now, please config states before start. currentState " + currentState + " obj " + t);
        if(stateObj != null && state != null) {
            stateObj.setState(state);
            stateMap.put(state, stateObj);
        }
        return this;
    }

    public StateMachine<K, T> errorOccurred(StateErrorOccurredExecutor<K, T> stateErrorOccurredExecutor) {
        this.stateErrorOccurredExecutor = stateErrorOccurredExecutor;
        return this;
    }

//    private void changeState(K toState, String message) {
//        logger.info(TAG, name + ": [changeState] " + message);
//        State<K, T> currentStateObj = stateMap.get(currentState);
//        StateExecutor<K, T> leaveStateExecutor = null;
//        if (currentStateObj != null)
//            leaveStateExecutor = currentStateObj.getLeaveStateExecutor();
//        K old = currentState;
//        currentState = toState;
//        if(leaveStateExecutor != null) {
//            try {
//                leaveStateExecutor.execute(t, this);
//            } catch(Throwable t) {
//                logger.error(TAG, name + ": Leave state from " + old + " to " + toState + " failed, " + t.getMessage() + " for leaveStateExecutor " + leaveStateExecutor);
//            }
//        }
//        if(stateListeners != null) {
//            stateListeners.forEach(stateListener -> {
//                try {
//                    stateListener.stateChanged(old, toState, t);
//                } catch(Throwable t) {
//                    logger.error(TAG, name + ": State changed callback failed, " + t.getMessage() + " for listener " + stateListener);
//                }
//            });
//        }
//    }
    public void gotoState(K state, String reason) {
        gotoState(state, reason, null);
    }

    public void gotoState(K state, String reason, StateBeforeExecutor<K, T> stateBeforeExecutor) {
        if(executorService != null) {
            executorService.submit(() -> {
                gotoStatePrivate(state, reason, stateBeforeExecutor);
            });
        } else {
            gotoStatePrivate(state, reason, stateBeforeExecutor);
        }
    }

    private synchronized void gotoStatePrivate(K state, String reason, StateBeforeExecutor<K, T> stateBeforeExecutor) {
        if(!stateMachineStated)
            stateMachineStated = true;
        State<K, T> stateObj = stateMap.get(state);
        if(stateObj == null) {
            IllegalArgumentException throwable = new IllegalArgumentException(name + ": State " + state + " is not configured, reason " + reason + " for obj " + t);
            if(stateErrorOccurredExecutor != null) {
                TapLogger.error(TAG, "{}: go to state {}, stateObj not exist", name, state);
                try {
                    stateErrorOccurredExecutor.onError(throwable, currentState, state, this.t, this);
                } catch (Throwable t1) {
                    TapLogger.error(TAG, "Execute state occurred error executor failed, [{}] state {} from state {} obj {} reason {}", t1.getMessage(), state, currentState, t, reason);
                }
                return;
            } else {
                throw throwable;
            }
        }

        Set<K> gotoStates = null;
        if(currentState != null) {
            State<K, T> currentStateObj = stateMap.get(currentState);
            if(currentStateObj == null) {
                IllegalStateException throwable = new IllegalStateException(name + ": CurrentState is illegal " + currentState + " maybe caused by modifying state configuration during running the state machine. Force currentState to be null. obj " + t + " reason " + reason);
                if(stateErrorOccurredExecutor != null) {
                    TapLogger.error(TAG, String.format("%s: go to state %s, currentStateObj not exist", name, state));
                    try {
                        stateErrorOccurredExecutor.onError(throwable, currentState, state, this.t, this);
                    } catch (Throwable t1) {
                        TapLogger.error(TAG, "Execute state occurred error executor failed, [{}] state {} from state {} obj {} reason {}", t1.getMessage(), state, currentState, t, reason);
                    }
                    return;
                } else {
                    throw throwable;
                }
            } else {
                gotoStates = currentStateObj.getGotoStates();
            }
        }

        if(gotoStates != null) {
            if(!gotoStates.contains(state)) {
                IllegalStateException throwable = new IllegalStateException(name + ": Current state is " + currentState + ", can NOT go to state " + state + " obj " + t + " reason " + reason);
                if(stateErrorOccurredExecutor != null) {
                    TapLogger.error(TAG, "{}: currentState {} go to state {}, not defined in nextStates: {}", name, currentState, state, gotoStates);
                    try {
                        stateErrorOccurredExecutor.onError(throwable, currentState, state, this.t, this);
                    } catch (Throwable t1) {
                        TapLogger.error(TAG, "Execute state occurred error executor failed, [{}] state {} from state {} obj {} reason {}", t1.getMessage(), state, currentState, t, reason);
                    }
                    return;
                } else {
                    throw throwable;
                }
            }
        }

        StateExecutor<K, T> executor = stateObj.getStateExecutor();
        K lastState = currentState;
        TapLogger.debug(TAG, "{}: [changeState] StateMachine currentState {} goes to {} successfully. reason {} obj {}", name, currentState, state, reason, t);
        currentState = state;
        if(stateBeforeExecutor != null) {
            try {
                stateBeforeExecutor.execute(this.t, this);
            } catch(Throwable t) {
                t.printStackTrace();
                TapLogger.error(TAG, "{}: State before executor from {} to {} failed, {} for stateBeforeExecutor {}", name, lastState, currentState, t.getMessage(), stateBeforeExecutor);
            }
        }
        if(executor != null) {
            try {
                State<K, T> currentStateObj = stateMap.get(lastState);
                StateExecutor<K, T> leaveStateExecutor = null;
                if (currentStateObj != null)
                    leaveStateExecutor = currentStateObj.getLeaveStateExecutor();
                if(leaveStateExecutor != null) {
                    try {
                        leaveStateExecutor.execute(t, this);
                    } catch(Throwable t) {
                        t.printStackTrace();
                        TapLogger.error(TAG, "{}: Leave state from {} to {} failed, {} for leaveStateExecutor {}", name, lastState, currentState, t.getMessage(), leaveStateExecutor);
                    }
                }
                executor.execute(this.t, this);

                if(stateListeners != null) {
                    stateListeners.forEach(stateListener -> {
                        try {
                            stateListener.stateChanged(lastState, currentState, t);
                        } catch(Throwable t) {
                            t.printStackTrace();
                            TapLogger.error(TAG, "{}: State changed callback failed, {} for listener {}", name, t.getMessage(), stateListener);
                        }
                    });
                }
            } catch(Throwable t) {
                t.printStackTrace();
                if(stateErrorOccurredExecutor != null) {
                    TapLogger.error(TAG, "{}: Execute state executor failed, [{}] state {} from state {} obj {} reason {} will invoke stateErrorOccurredExecutor#onError method", name, t.getMessage(), state, lastState, t, reason);
                    try {
                        stateErrorOccurredExecutor.onError(t, lastState, state, this.t, this);
                    } catch (Throwable t1) {
                        currentState = lastState;
                        TapLogger.error(TAG, "Execute state occurred error executor failed, [{}] state {} from state {} obj {} reason {} will change back to last state {}", t1.getMessage(), state, lastState, t, reason, lastState);
                    }
                } else {
                    currentState = lastState;
                    TapLogger.error(TAG, "Execute state executor failed, [{}] state {} from state {} obj {} reason {} will change back to last state {} because no stateErrorOccurredExecutor", t.getMessage(), state, lastState, t, reason, lastState);
                }
            }
        }
    }

    public K getCurrentState() {
        return currentState;
    }
}
