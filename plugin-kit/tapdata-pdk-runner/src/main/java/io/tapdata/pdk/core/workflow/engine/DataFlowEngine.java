package io.tapdata.pdk.core.workflow.engine;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.Validator;
import io.tapdata.pdk.core.utils.state.StateListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataFlowEngine {
    private static final String TAG = DataFlowEngine.class.getSimpleName();
    private static final DataFlowEngine instance = new DataFlowEngine();
    private Map<String, DataFlowWorker> idDataFlowWorkerMap = new ConcurrentHashMap<>();
    public static DataFlowEngine getInstance() {
        return instance;
    }

    public final String version = "0.9.0";

    private DataFlowEngine() {}
    /**
     * 引擎启动
     */
    public void start() {
        TapLogger.info(TAG, ".___________.    ___      .______    _______       ___   .___________.    ___     ");
        TapLogger.info(TAG, "|           |   /   \\     |   _  \\  |       \\     /   \\  |           |   /   \\    ");
        TapLogger.info(TAG, "`---|  |----`  /  ^  \\    |  |_)  | |  .--.  |   /  ^  \\ `---|  |----`  /  ^  \\   ");
        TapLogger.info(TAG, "    |  |      /  /_\\  \\   |   ___/  |  |  |  |  /  /_\\  \\    |  |      /  /_\\  \\  ");
        TapLogger.info(TAG, "    |  |     /  _____  \\  |  |      |  '--'  | /  _____  \\   |  |     /  _____  \\ ");
        TapLogger.info(TAG, "    |__|    /__/     \\__\\ | _|      |_______/ /__/     \\__\\  |__|    /__/     \\__\\");
        TapLogger.info(TAG, "                                                                            v{}", version);
        //http://www.network-science.de/ascii/
        //starwars

    }

    public void startDataFlow(TapDAG dag, JobOptions jobOptions) {
        startDataFlow(dag, jobOptions, null);
    }
    public DataFlowWorker startDataFlow(TapDAG dag, JobOptions jobOptions, StateListener<String, DataFlowWorker> stateListener) {
        Validator.checkNotNull(PDKRunnerErrorCodes.MAIN_DAG_IS_ILLEGAL, dag);
        Validator.checkAllNotNull(PDKRunnerErrorCodes.MAIN_DAG_IS_ILLEGAL, dag.getId());

        if(!idDataFlowWorkerMap.containsKey(dag.getId())) {
            DataFlowWorker dataFlowWorker = new DataFlowWorker();
            DataFlowWorker old = idDataFlowWorkerMap.putIfAbsent(dag.getId(), dataFlowWorker);
            if(old == null) {
                //Ensure only one dataFlowWorker can be started.
                dataFlowWorker.init(dag, jobOptions);
                if(stateListener != null)
                    dataFlowWorker.addStateListener(stateListener);
                dataFlowWorker.start();
                return dataFlowWorker;
            }
        } else {
            TapLogger.error(TAG, "DAG Id {} is running, can not startDataFlow again.", dag.getId());
        }
        return null;
    }

    public void sendExternalTapEvent(String dagId, String nodeId, TapEvent event) {
        DataFlowWorker dataFlowWorker = idDataFlowWorkerMap.get(dagId);
        if(dataFlowWorker != null) {
            dataFlowWorker.sendExternalEvent(event, nodeId);
        }
    }

    public void sendExternalTapEvent(String dagId, TapEvent event) {
        DataFlowWorker dataFlowWorker = idDataFlowWorkerMap.get(dagId);
        if(dataFlowWorker != null) {
            dataFlowWorker.sendExternalEvent(event);
        }
    }

    public boolean stopDataFlow(String dagId) {
        Validator.checkNotNull(PDKRunnerErrorCodes.MAIN_DAG_IS_ILLEGAL, dagId);

        DataFlowWorker dataFlowWorker = idDataFlowWorkerMap.remove(dagId);
        if(dataFlowWorker == null)
            return false;
//            throw new CoreException(ErrorCodes.MAIN_DATAFLOW_NOT_FOUND, "DAG " + dagId + " doesn't be found");

        dataFlowWorker.stop();
        return true;
    }

    /**
     * 引擎停止
     */
    public void stop() {
        for(DataFlowWorker dataFlowWorker : idDataFlowWorkerMap.values()) {
            CommonUtils.ignoreAnyError(() -> dataFlowWorker.stop(), TAG);
        }
    }

    public Map<String, DataFlowWorker> getIdDataFlowWorkerMap() {
        return idDataFlowWorkerMap;
    }

    public void setIdDataFlowWorkerMap(Map<String, DataFlowWorker> idDataFlowWorkerMap) {
        this.idDataFlowWorkerMap = idDataFlowWorkerMap;
    }

}
