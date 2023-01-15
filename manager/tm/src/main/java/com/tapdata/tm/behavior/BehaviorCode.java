package com.tapdata.tm.behavior;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/6/22 上午11:27
 */
public enum BehaviorCode {

    createConnection,
    editConnection,
    testConnection,

    createTask,
    editTask,
    deleteTask,

    taskStatusChange,

    createDataFlow,
    editDataFlow,
    resetDataFlow,
    deleteDataFlow,
    startDataFlow,
    stopDataFlow,
    forceStopDataFlow,
    errorDataFlow,
    pausedDataFlow,
    statsForDataFlowInsight,

}
