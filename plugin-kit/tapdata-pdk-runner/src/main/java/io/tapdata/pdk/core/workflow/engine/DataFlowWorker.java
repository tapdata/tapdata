package io.tapdata.pdk.core.workflow.engine;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.Validator;
import io.tapdata.pdk.core.utils.state.StateListener;
import io.tapdata.pdk.core.utils.state.StateMachine;
import io.tapdata.pdk.core.workflow.engine.driver.ProcessorNodeDriver;
import io.tapdata.pdk.core.workflow.engine.driver.SourceNodeDriver;
import io.tapdata.pdk.core.workflow.engine.driver.SourceStateListener;
import io.tapdata.pdk.core.workflow.engine.driver.TargetNodeDriver;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class DataFlowWorker {
    private static final String TAG = DataFlowWorker.class.getSimpleName();

    private TapDAG dag;
    private JobOptions jobOptions;
    public static final String STATE_NONE = "None";
    public static final String STATE_INITIALIZING = "Initializing";
    public static final String STATE_INITIALIZED = "Initialized";
    public static final String STATE_INITIALIZE_FAILED = "Initialize failed";
    public static final String STATE_TABLE_PREPARED = "Table prepared";
    public static final String STATE_TERMINATED = "Terminated";
    private StateMachine<String, DataFlowWorker> stateMachine;
    private AtomicBoolean started = new AtomicBoolean(false);
    private LastError lastError;
//    private SingleThreadQueue<Runnable> singleThreadQueue;
    private ExecutorService workerThread;
    private SourceStateListener sourceStateListener;

    public static class LastError {
        private CoreException coreException;
        private String fromState;
        private String toState;

        public LastError(CoreException coreException, String fromState, String toState) {
            this.coreException = coreException;
            this.fromState = fromState;
            this.toState = toState;
        }

        public CoreException getCoreException() {
            return coreException;
        }

        public void setCoreException(CoreException coreException) {
            this.coreException = coreException;
        }

        public String getFromState() {
            return fromState;
        }

        public void setFromState(String fromState) {
            this.fromState = fromState;
        }

        public String getToState() {
            return toState;
        }

        public void setToState(String toState) {
            this.toState = toState;
        }
    }

    DataFlowWorker() {
    }

    private void initStateMachine() {
        if(stateMachine == null) {
            stateMachine = new StateMachine<>(DataFlowWorker.class.getSimpleName() + "#" + dag.getId(), STATE_NONE, this);
//        getServiceNodesHandler = StateOperateRetryHandler.build(stateMachine, CoreRuntime.getExecutorsManager().getSystemScheduledExecutorService()).setMaxRetry(5).setRetryInterval(2000L)
//                .setOperateListener(this::handleGetServicesNodes)
//                .setOperateFailedListener(this::handleGetServicesNodesFailed);
            stateMachine.configState(STATE_NONE, stateMachine.execute().nextStates(STATE_INITIALIZING, STATE_TERMINATED))
                    .configState(STATE_INITIALIZING, stateMachine.execute(this::handleInitializing).nextStates(STATE_TABLE_PREPARED, STATE_INITIALIZED, STATE_TERMINATED, STATE_INITIALIZE_FAILED))
                    .configState(STATE_INITIALIZE_FAILED, stateMachine.execute(this::handleInitializeFailed).nextStates(STATE_INITIALIZING, STATE_TERMINATED))
                    .configState(STATE_TABLE_PREPARED, stateMachine.execute(this::handleRecordsSent).nextStates(STATE_INITIALIZED, STATE_TERMINATED))
                    .configState(STATE_INITIALIZED, stateMachine.execute(this::handleInitialized).nextStates(STATE_TABLE_PREPARED, STATE_TERMINATED))
                    .configState(STATE_TERMINATED, stateMachine.execute(this::handleTerminated).nextStates(STATE_INITIALIZING, STATE_NONE))
                    .errorOccurred(this::handleError);
            stateMachine.enableAsync(Executors.newSingleThreadExecutor()); //Use one thread for a worker
        }
    }

    private void handleRecordsSent(DataFlowWorker dataFlowWorker, StateMachine<String, DataFlowWorker> stringDataFlowWorkerStateMachine) {

    }

    private void handleInitializeFailed(DataFlowWorker dataFlowWorker, StateMachine<String, DataFlowWorker> stringDataFlowWorkerStateMachine) {
    }

    private void handleTerminated(DataFlowWorker dataFlowWorker, StateMachine<String, DataFlowWorker> integerDataFlowWorkerStateMachine) {
    }

    private void handleInitialized(DataFlowWorker dataFlowWorker, StateMachine<String, DataFlowWorker> integerDataFlowWorkerStateMachine) {
        //Start head nodes
        List<String> headNodeIds = dag.getHeadNodeIds();
        for(String nodeId : headNodeIds) {
            TapDAGNodeEx nodeWorker = dag.getNodeMap().get(nodeId);
            if(nodeWorker.sourceNodeDriver != null) {
                try {
                    nodeWorker.sourceNodeDriver.getSourceNode().applyClassLoaderContext(() -> nodeWorker.sourceNodeDriver.start(state -> {
                        if(sourceStateListener != null) {
                            CommonUtils.ignoreAnyError(() -> {
                                sourceStateListener.stateChanged(state);
                            }, TAG);
                        }
                        if(state == SourceNodeDriver.STATE_TABLE_PREPARED) {
                            stateMachine.gotoState(STATE_TABLE_PREPARED, "Target table should have been prepared");
                        }
                    }));
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                    //TODO
                }
            }
        }
    }
    public void sendExternalEvent(TapEvent event) {
        sendExternalEvent(event, null);
    }
    public void sendExternalEvent(TapEvent event, String sourceNodeId) {
        if(sourceNodeId != null) {
            TapDAGNodeEx nodeWorker = dag.getNodeMap().get(sourceNodeId);
            if(nodeWorker != null) {
                if(nodeWorker.sourceNodeDriver != null) {
//                    nodeWorker.sourceNodeDriver.getSourceNode().offerExternalEvent(event);
                    nodeWorker.sourceNodeDriver.receivedExternalEvent(Collections.singletonList(event));//offer(Collections.singletonList(event));
                } else {
                    TapLogger.warn(TAG, "External event can only send from source node, the nodeId {} is not a source node", sourceNodeId);
                }
                //Processor not consider at this moment.
            }
        } else {
            List<String> headNodeIds = dag.getHeadNodeIds();
            for(String theNodeId : headNodeIds) {
                TapDAGNodeEx nodeWorker = dag.getNodeMap().get(theNodeId);
                if(nodeWorker.sourceNodeDriver != null) {
                    nodeWorker.sourceNodeDriver.receivedExternalEvent(Collections.singletonList(event));//offer(Collections.singletonList(event));
                }
            }
        }
    }

    private void handleInitializing(DataFlowWorker dataFlowWorker, StateMachine<String, DataFlowWorker> integerDataFlowWorkerStateMachine) {
        List<String> headNodeIds = dag.getHeadNodeIds();
        checkAllNodesInDAG(dag);

        //Setup all nodes, build path for nodes.
        for(String nodeId : headNodeIds) {
            TapDAGNodeEx nodeWorker = dag.getNodeMap().get(nodeId);
            nodeWorker.setup(dag, jobOptions);
        }

        stateMachine.gotoState(STATE_INITIALIZED, "DataFlow " + dag.getId() + " init successfully");
    }

    private void checkAllNodesInDAG(TapDAG dagNodes) {
        //TODO check all node information are correct, PDK can be found correctly.
        //If startNodes not source, or tail nodes not target, remove them.
    }

    public SourceNodeDriver getSourceNodeDriver(String nodeId) {
        TapDAGNodeEx nodeWorker = dag.getNodeMap().get(nodeId);
        if(nodeWorker != null) {
            return nodeWorker.sourceNodeDriver;
        }
        return null;
    }

    public TargetNodeDriver getTargetNodeDriver(String nodeId) {
        TapDAGNodeEx nodeWorker = dag.getNodeMap().get(nodeId);
        if(nodeWorker != null) {
            return nodeWorker.targetNodeDriver;
        }
        return null;
    }

    public ProcessorNodeDriver getProcessorNodeDriver(String nodeId) {
        TapDAGNodeEx nodeWorker = dag.getNodeMap().get(nodeId);
        if(nodeWorker != null) {
            return nodeWorker.processorNodeDriver;
        }
        return null;
    }

    public synchronized void start() {
        if(stateMachine.getCurrentState().equals(STATE_NONE)) {
            if(started.compareAndSet(false, true)) {
                //Init every nodes in this data flow.
                stateMachine.gotoState(STATE_INITIALIZING, "Dataflow " + dag.getId() + " start initializing");
            }
        } else {
            throw new CoreException(PDKRunnerErrorCodes.MAIN_DATAFLOW_WORKER_ILLEGAL_STATE, "Dataflow worker state is illegal to init, current state is " + stateMachine.getCurrentState() + " expect state " + STATE_NONE);
        }
    }

    private void handleError(Throwable throwable, String fromState, String toState, DataFlowWorker dataFlowWorker, StateMachine<String, DataFlowWorker> stateMachine) {
        lastError = new LastError(CommonUtils.generateCoreException(throwable), fromState, toState);
    }

    public synchronized void stop() {
        if(dag != null) {
            List<String> headNodeIds = dag.getHeadNodeIds();
            for(String nodeId : headNodeIds) {
                TapDAGNodeEx nodeWorker = dag.getNodeMap().get(nodeId);
                destroyNode(nodeWorker);
            }
        }
    }

    private void destroyNode(TapDAGNodeEx nodeWorker) {
        if(nodeWorker == null) return;
        List<String> childNodeIds = nodeWorker.getChildNodeIds();
        if(childNodeIds != null) {
            for(String childNodeId : childNodeIds) {
                destroyNode(dag.getNodeMap().get(childNodeId));
            }
        }
        PDKIntegration.releaseAssociateId(nodeWorker.getId());
        if(nodeWorker.sourceNodeDriver != null) {
            CommonUtils.ignoreAnyError(() -> nodeWorker.sourceNodeDriver.destroy(), TAG);
            nodeWorker.sourceNodeDriver = null;
        }
        if(nodeWorker.processorNodeDriver != null) {
            CommonUtils.ignoreAnyError(() -> nodeWorker.processorNodeDriver.destroy(), TAG);
            nodeWorker.processorNodeDriver = null;
        }
        if(nodeWorker.targetNodeDriver != null) {
            CommonUtils.ignoreAnyError(() -> nodeWorker.targetNodeDriver.destroy(), TAG);
            nodeWorker.targetNodeDriver = null;
        }
    }

    public synchronized void init(TapDAG newDag, JobOptions jobOptions) {
        Validator.checkNotNull(PDKRunnerErrorCodes.MAIN_DAG_IS_ILLEGAL, newDag);
        Validator.checkAllNotNull(PDKRunnerErrorCodes.MAIN_DAG_IS_ILLEGAL, newDag.getId(), newDag.getHeadNodeIds());

        if(this.dag == null)
            this.dag = newDag;
        else
            throw new CoreException(PDKRunnerErrorCodes.MAIN_DAG_WORKER_STARTED_ALREADY, "DAG worker has started for data flow " + this.dag.getId() + ", the data flow " + newDag.getId() + " can not start again");

        if(jobOptions == null)
            jobOptions = new JobOptions();

        this.jobOptions = jobOptions;
        initStateMachine();
    }

    public String getCurrentState() {
        if(stateMachine != null) {
            return stateMachine.getCurrentState();
        }
        return STATE_NONE;
    }

    public void addStateListener(StateListener<String, DataFlowWorker> stateListener) {
        if(stateMachine != null) {
            stateMachine.addStateListener(stateListener);
        }
    }

    public void removeStateListener(StateListener<String, DataFlowWorker> stateListener) {
        if(stateMachine != null) {
            stateMachine.removeStateListener(stateListener);
        }
    }

    public SourceStateListener getSourceStateListener() {
        return sourceStateListener;
    }

    public void setSourceStateListener(SourceStateListener sourceStateListener) {
        this.sourceStateListener = sourceStateListener;
    }
}
