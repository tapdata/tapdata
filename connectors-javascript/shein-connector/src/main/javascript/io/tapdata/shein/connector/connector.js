
config.setStreamReadIntervalSeconds(2*60);

function discoverSchema(connectionConfig) {
    return Object.values(globalTableConfig);
}

function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    tableHandle.handle(tableName).batchReadV(connectionConfig, nodeConfig, offset, pageSize, batchReadSender);
}

function streamRead(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {
    if (!isValue(tableNameList)){
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
            tableHandel.streamReadV(connectionConfig, nodeConfig, offset, pageSize, streamReadSender);
        }
    }
}

function connectionTest(connectionConfig) {
    let openKeyId = connectionConfig.openKeyId;
    let checkItems = [];
    if (!isParam(openKeyId) || null == openKeyId || "" === openKeyId.trim()){
        checkItems.push({
            "test": "Check your Open Key ID",
            "code": -1,
            "result": "Please make sure your openKeyId not empty"
        });
        return checkItems;
    }else {
        checkItems.push({
            "test": "Check your Open Key ID",
            "code": 1,
            "result": "Pass"
        });
    }

    let secretKey = connectionConfig.secretKey;
    if (!isParam(secretKey) || null == secretKey || "" === secretKey.trim()){
        checkItems.push({
            "test": "Check your Secret Key",
            "code": -1,
            "result": "Please make sure your Secret Key not empty"
        });
        return checkItems;
    }else {
        checkItems.push({
            "test": "Check your Secret Key",
            "code": 1,
            "result": "Pass"
        });
    }

    let doMain = connectionConfig.doMain;
    if (!isParam(doMain) || null == doMain || "" === doMain.trim()){
        checkItems.push({
            "test": "Check your domain URL",
            "code": -1,
            "result": "Please make sure your domain URL not empty"
        });
        return checkItems;
    }else {
        checkItems.push({
            "test": "Check your domain URL",
            "code": 1,
            "result": "Pass"
        });
    }

    let timeStamp = new Date().getTime();
    let signatureRule = getSignatureRules(openKeyId, secretKey,"/open-api/order/purchase-order-infos", timeStamp);
    let goods = invoker.invoke("Shopping", {
        "pageNumber": 1,
        "x-lt-signature": signatureRule,
        "x-lt-timestamp": BigInt(timeStamp),
        "updateTimeStart": '1970-01-01 00:00:00',
        "updateTimeEnd": "2100-01-01 23:59:59"
    });
    if (!isParam(goods) || null == goods || !isParam(goods.result) || null == goods.result){
        checkItems.push({
            "test": "Debugging the Order API",
            "code": -1,
            "result": "Can not get any order with http response by your Open Key ID and Secret Key and domain URL"
        });
        return checkItems;
    }else {
        checkItems.push({
            "test": " Debugging the Order API",
            "code": 1,
            "result": "Pass"
        });
    }
    let result = goods.result;
    let pageInfo = result.info;
    if (!isValue(pageInfo)){
        checkItems.push({
            "test":"Debugging the Order API and Check API result",
            "code": -1,
            "result": "Can not get order list " +
                ( isValue(result.msg) ? (", msg: " + result.msg) : "" ) +
                ( isValue(result.error) ? (", error: " + result.error) : "")
        });
        return checkItems;
    }else {
        checkItems.push({
            "test":"",
            "code": 1,
            "result": "Pass"
        });
    }
    return checkItems;
}

function updateToken(connectionConfig, nodeConfig, apiResponse) {
    return null;
}