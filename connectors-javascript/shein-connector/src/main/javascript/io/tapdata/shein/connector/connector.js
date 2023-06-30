
config.setStreamReadIntervalSeconds(2*60);

function discoverSchema(connectionConfig) {
    let tableType = connectionConfig.tableType;
    if (isValue(tableType) && "Document" === tableType){
        return Object.values(globalTableConfig);
    }
    return Object.values(globalCSVTableConfig);
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
        if (null == handle){
            log.warn("Table can not be support stream read, and will ignore table which name is {}", tableNameList[index]);
            continue;
        }
        tableHandles.push(handle);
    }
    //while (isAlive()){
    for (let index = 0; index < tableHandles.length; index++) {
        if (!isAlive()) break;
        let tableHandel = tableHandles[index];
        tableHandel.streamReadV(connectionConfig, nodeConfig, offset, pageSize, streamReadSender);
    }
    //}
}

function connectionTest(connectionConfig) {
    let openKeyId = connectionConfig.openKeyId;
    let checkItems = [];
    let isCheck = !isParam(openKeyId) || null == openKeyId || "" === openKeyId.trim();
    checkItems.push({"test": "Check your Open Key ID",
        "code": isCheck ? -1 : 1,
        "result": isCheck ? "Please make sure your openKeyId not empty" : "Pass"
    });
    if (isCheck) return checkItems;

    let secretKey = connectionConfig.secretKey;
    isCheck = !isParam(secretKey) || null == secretKey || "" === secretKey.trim();
    checkItems.push({"test": "Check your Secret Key",
        "code": isCheck ? -1 : 1,
        "result": isCheck ? "Please make sure your Secret Key not empty" : "Pass"
    });
    if (isCheck) return checkItems;

    let doMain = connectionConfig.doMain;
    isCheck = !isParam(doMain) || null == doMain || "" === doMain.trim();
    checkItems.push({"test": "Check your domain URL",
        "code": isCheck ? -1 : 1,
        "result": isCheck ? "Please make sure your domain URL not empty" : "Pass"
    });
    if (isCheck) return checkItems;

    let timeStamp = new Date().getTime();
    let signatureRule = getSignatureRules(openKeyId, secretKey,"/open-api/order/purchase-order-infos", timeStamp);
    let goods = invoker.invoke("Shopping", {
        "pageNumber": 1, "pageSize": 10,
        "x-lt-signature": signatureRule,
        "x-lt-timestamp": BigInt(timeStamp),
        "updateTimeStart": '1970-01-01 00:00:00',
        "updateTimeEnd": "2100-01-01 23:59:59"
    });
    isCheck = !isParam(goods) || null == goods || !isParam(goods.result) || null == goods.result;
    checkItems.push({"test": "Debugging the Order API",
        "code": isCheck ? -1 : 1,
        "result": isCheck ? "Can not get any order with http response by your Open Key ID and Secret Key and domain URL" : "Pass"
    });
    if (isCheck) return checkItems;

    let result = goods.result;
    let pageInfo = result.info;
    isCheck = !isValue(pageInfo);
    checkItems.push({"test":"Debugging the Order API and Check API result",
        "code": isCheck ? -1 : 1,
        "result": isCheck? ("Can not get order list, http code: " + goods.httpCode + " " +
                        ( isValue(result.msg) ? (", msg: " + result.msg) : "" ) +
                        ( isValue(result.error) ? (", error: " + result.error) : ""))
                    : "Pass"
    });

    if (isCheck) {
        checkItems.push({
            "test": "Read log",
            "code": 1,
            "result": "Pass"
        });
    }
    return checkItems;
}

function updateToken(connectionConfig, nodeConfig, apiResponse) {
    return null;
}