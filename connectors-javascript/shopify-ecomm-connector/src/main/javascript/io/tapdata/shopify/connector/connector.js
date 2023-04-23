var batchStart = dateUtils.nowDate();


function discoverSchema(connectionConfig) {
    return ['example_table'];
}


function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {

}



function streamRead(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {

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

// function insertRecord(connectionConfig, nodeConfig, eventDataMap){
//
// }

// function updateRecord(connectionConfig, nodeConfig, eventDataMap){
//
// }

// function deleteRecord(connectionConfig, nodeConfig, eventDataMap){
//
// }

function updateToken(connectionConfig, nodeConfig, apiResponse) {
    // if (apiResponse.code === 401) {
    //     let result = invoker.invokeV2("apiName");
    //     return {"access_token": result.result.token};
    // }
    // return null;
}

