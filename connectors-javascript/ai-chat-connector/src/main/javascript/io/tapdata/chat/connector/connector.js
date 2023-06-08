var invoker = loadAPI();
function discoverSchema(connectionConfig) { return ['default'];}
function connectionTest(connectionConfig) {
    let tag = 1, msg = "Successfully";
    try {
        let result = invoker.invokeWithoutIntercept("Add Content");
        if (result.httpCode < 200 || result.httpCode >= 300) {
            tag = -1;
            msg = "Your token is incorrect or the URL you entered is incorrect.";
        }
    }catch (e) {
        tag = -1;
        msg = "Your token is incorrect or the URL you entered is incorrect, " + exceptionUtil.eMessage(e);
    }
    return [{"test": "Check your host URL and Token", "code": tag, "result": msg}];
}
function insertRecord(connectionConfig, nodeConfig, eventDataMap, tableJsonString){
    return writeData(eventDataMap, tableJsonString, true)
}
function updateRecord(connectionConfig, nodeConfig, eventDataMap, tableJsonString){
    return writeData(eventDataMap, tableJsonString, false)
}
function writeData(eventDataMap, tableJsonString, isInsert){
    let tableMap = JSON.parse(tableJsonString);
    let csvData = csvUtils.format([eventDataMap.afterData]);
    let bodyConfig = {
        "data_id" : tableMap.keys[0],
        "content" : csvData,
        "append" : isInsert,
        "table" : eventDataMap.tableName };
    try{
        let httpResult = invoker.invokeWithoutIntercept("Add Content", bodyConfig);
        if (httpResult.httpCode < 200 || httpResult.httpCode >= 300) {
            log.warn("Failed to {} a record, record: {}.", isInsert ? "insert" : "update", csvData);
            return false;
        }
        return true;
    }catch (e){
        log.warn("Fail to {} record, record: {}, msg: {}", isInsert ? "insert" : "update", csvData, exceptionUtil.eMessage(e));
        return false;
    }
}
function connectorWebsite(connectionConfig){
    return {
        'url': connectionConfig.url
    }
}
function tableWebsite(connectionConfig, tableIdList){
    if ('undefined' === tableIdList || null == tableIdList || tableIdList.length <= 0){
        return '';
    }
    return {
        // "urls":{
        //     "table1":{
        //         'url': "sadf"
        //     },
        //     "table2":{
        //         'url': "sadf"
        //     }
        // },
        "url": connectionConfig.url + "/ns_" + tableIdList[0]
    }
}