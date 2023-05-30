function discoverSchema(connectionConfig) {
    return Object.keys(globalTableConfig);
}

function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    tableHandle.handle(tableName).batchRead(connectionConfig, nodeConfig, offset, pageSize, batchReadSender);
}

function streamRead(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {
    if (!arrayUtils.isNotEmptyArray(tableNameList)){
        throw ("Can not get any table name, table name list is empty");
    }
    let tableHandles = [];
    for (let index = 0; index < tableNameList.length; index++) {
        let handle = tableHandle.handle(tableNameList[index]);
        if (handle instanceof DefaultTable){
            log.warn("Table can not be support stream read, and will ignore table which name is {}", tableNameList[index]);
        }
        tableHandles.push(handle);
    }
    while (isAlive()){
        for (let index = 0; index < tableHandles.length; index++) {
            if (!isAlive()) break;
            let tableHandel = tableHandles[index];
            tableHandel.streamRead(connectionConfig, nodeConfig, offset, pageSize, streamReadSender);
        }
    }
}

function connectionTest(connectionConfig) {
    return [{
        "test": "Example test item",
        "code": 1,
        "result": "Pass"
    }];
}

function commandCallback(connectionConfig, nodeConfig, commandInfo) {

}

function webhookEvent(connectionConfig, nodeConfig, tableNameList, eventDataMap) {

    //return [
    //     {
    //         "eventType": "i/u/d",
    //         "tableName": "${example_table_name}",
    //         "referenceTime": Number(),
    //         "afterData": {},
    //         "beforeData":{}
    //     }
    //]
}

function writeRecord(connectionConfig, nodeConfig, eventDataList, writerResultCollector) {

    //Please use writerResultCollector.markSent(eventData(s)) to collect data after each successful data processing
    //writerResultCollector.markSent(eventData(s))
}

function updateToken(connectionConfig, nodeConfig, apiResponse) {
    return null;
}

