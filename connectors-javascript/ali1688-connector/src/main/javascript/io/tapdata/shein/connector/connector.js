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
        if (null == handle){
            log.warn("Table can not be support stream read, and will ignore table which name is {}", tableNameList[index]);
            continue;
        }
        tableHandles.push(handle);
    }
    for (let index = 0; index < tableHandles.length; index++) {
        if (!isAlive()) break;
        let tableHandel = tableHandles[index];
        tableHandel.streamReadV(connectionConfig, nodeConfig, offset, pageSize, streamReadSender);
    }
}

function connectionTest(connectionConfig) {
    let appKey = connectionConfig.appKey;
    let checkItems = [];
    let isCheck = !isParam(appKey) || null == appKey || "" === appKey.trim();
    checkItems.push({"test": "Check App Key ID",
        "code": isCheck ? -1 : 1,
        "result": isCheck ? " The App Key has expired. Please contact technical support personnel " : "Pass"
    });
    if (isCheck) return checkItems;

    let secretKey = connectionConfig.secretKey;
    isCheck = !isParam(secretKey) || null == secretKey || "" === secretKey.trim();
    checkItems.push({"test": "Check App Secret",
        "code": isCheck ? -1 : 1,
        "result": isCheck ? " The App Secret has expired. Please contact technical support personnel " : "Pass"
    });
    if (isCheck) return checkItems;

    let timeStamp = new Date().getTime();
    let accessToken = getConfig("access_token");
    let signatureRule = getSignatureRules(secretKey,"param2/1/cn.alibaba.open/com.alibaba.trade/alibaba.trade.fastCreateOrder-1/" + apiKey, {
        "_aop_timestamp": timeStamp,
        "access_token": accessToken
    });
    let goods = invoker.invoke("Shopping", {
        "page": 1,
        "pageSize": 1,
        "_aop_signature": signatureRule,
        "_aop_timestamp": BigInt(timeStamp)
    });

    isCheck = !isParam(goods) || null == goods || !isParam(goods.result) || null == goods.result;
    checkItems.push({"test": "Debugging the Order API",
        "code": isCheck ? -1 : 1,
        "result": isCheck ? "Can not get any order with http response" : "Pass"
    });
    if (isCheck) return checkItems;

    let result = goods.result;
    let pageInfo = result.result;
    isCheck = !isValue(pageInfo);
    checkItems.push({"test":"Debugging the Order API and Check API result",
        "code": isCheck ? -1 : 1,
        "result": isCheck? ("Can not get order list, http code " +
                goods.httpCode +
                (isValue(result.error_code)?(", error_code:" + result.error_code) : "") +
                (isValue(result.error_message)? (", error_message: " + result.error_message) : "") +
                (isValue(result.exception) ? (", exception: " + result.exception) : ""))
            : "Pass"
    });
    return checkItems;
}

function updateToken(connectionConfig, nodeConfig, apiResponse) {
    let apiMessage = apiResponse.result.errorMessage;
    if ( !(isValue(apiMessage) && "User authorization is required" === apiMessage.trim()) &&
        !(isValue(apiMessage) && "Request need user authorized" === apiMessage.trim()) ){
        return null;
    }

    let appKey = connectionConfig.appKey;
    if (!isParam(apiKey) || null == apiKey || "" === apiKey.trim()){
        log.error("The App Key has expired. Please contact technical support personnel");
        return null;
    }
    let token = connectionConfig.refresh_token;
    if (!isParam(secretKey) || null == secretKey || "" === secretKey.trim()){
        log.error("The refresh token has expired. Please re authorize your connection source ");
        return null;
    }
    let appSecret = connectionConfig.secretKey;
    if (!isParam(secretKey) || null == secretKey || "" === secretKey.trim()){
        log.error("The App Secret has expired. Please contact technical support personnel");
        return null;
    }
    let invoke = invoker.invoke("刷新AccessToken",{
        "timestamp": BigInt(new Date().getTime()),
        "appKey": appKey,
        "appSecret": appSecret,
        "refresh_token": token
    });
    let result = invoke.result;
    if (isValue(result) && isValue(result.access_token)){
        return {"access_token" : invoke.accessToken}
    }
    log.error("Can not refresh your access token when your access token has expired , http code: {} {}",
        invoke.httpCode ,
        (isValue(result.errorCode)?(", errorCode:" + result.errorCode) : ""),
        (isValue(result.errorMessage)?(", errorMessage:" + result.errorMessage) : "")
    );
    return null;
}

//1：Oauth授权， 构造签名因子：拼装的参数
//client_id=8668585&site=1688&redirect_uri=https://redirect.tapdata.io/oauth/complete/ali1688&state=1688
//client_id8668585redirect_urihttps://redirect.tapdata.io/oauth/complete/ali1688site1688state1688

//2：使用J8uHFtF3MHy对签名因子加密，io.tapdata.js.connector.base.JsUtil
//186DE0E262390646944ADBDB671EE2346759E18E53423B34E834932A9EAAB224
