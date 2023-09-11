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
    },{
        "test": "Read log",
        "code": 1,
        "result": "Pass"
    },{
        "test": "Read",
        "code": 1,
        "result": "Pass"
    }];
}

function commandCallback(connectionConfig, nodeConfig, commandInfo) {
    if (!isValue(commandInfo)){
        log.info("Not any command info, can not exec command")
        return ;
    }
    let command = commandInfo.command;
    if (!isValue(command)){
        log.info("Command type is empty, unable to execute command");
        return ;
    }
    switch (command){
        case 'OAuth':
            let config = connectionConfig.get("__TAPDATA_CONFIG");
            //log.warn("Value: appKey- {} , secretKey- {}, code- {}", config.get("appKey"), config.get("secretKey"), config.get("code"));
            let clientSecret = config.get("secretKey");
            let timestamp = BigInt(new Date().getTime());
            let clientInfo = {
                "type": "pdd.pop.auth.token.create",
                "data_type": "JSON",
                "client_id": config.get("clientId"),
                "timestamp": timestamp,
                "redirect_uri": config.get("redirectUri"),
                "code": connectionConfig.get("code")
            };
            clientInfo.sign = signURL(clientInfo, clientSecret);
            let getToken = invoker.invokeWithoutIntercept("GetTokenByCode", clientInfo);

            if (isValue(getToken.result) && isValue(getToken.result.pop_auth_token_create_response)) {
                let result = getToken.result.pop_auth_token_create_response;
                connectionConfig.refresh_token = result.refresh_token;
                connectionConfig.access_token = result.access_token;
                connectionConfig.scope = result.scope;
            } else {
                throw (isValue(getToken.error) ? getToken.error : "OAuth error") +
                (isValue(getToken.error_description)?(", errorMessage:" + getToken.error_description) : "")
            }
            //log.warn("code: {}", JSON.stringify(getToken.result))
            return connectionConfig;
    }
}

/**
 * "error_response": {
    "error_msg": "公共参数错误:type",
    "sub_msg": "",
    "sub_code": null,
    "error_code": 10001,
    "request_id": "15440104776643887"
  }
 * */
//主错误码	主错误描述	子错误码	子错误描述	解决办法
//10019	access_token已过期	10019	access_token已过期	刷新access_token或者重新授权再次获取access_token
function updateToken(connectionConfig, nodeConfig, apiResponse) {
    let result = apiResponse.result;
    let errorCode = 0;
    let subCode = 0;
    let errorResponse = result.error_response;
    if (isValue(errorResponse)){
        errorCode = errorResponse.error_code;
        subCode = errorResponse.sub_code;
    } else {
        errorCode  = result.error_code;
    }
    if (10019 == errorCode || 10019 == subCode){
        //重新获取access_token
        let key = connectionConfig.secretKey;
        if (!isValue(key)) {
            log.error("Can not get secretKey from connection config, secretKey is empty.")
            return null;
        }
        let clientId = connectionConfig.clientId;
        if (!isValue(clientId)) {
            log.error("Can not get clientId from connection config, clientId is empty.")
            return null;
        }
        let refreshToken = connectionConfig.refresh_token;
        if (!isValue(clientId)) {
            log.error("Can not get refreshToken from connection config, refreshToken is empty.")
            return null;
        }
        let timestamp = BigInt(new Date().getTime());
        let apiParam = {
            "type": "pdd.pop.auth.token.refresh",
            "data_type": "JSON",
            "client_id": clientId,
            "timestamp": timestamp,
            "refresh_token": refreshToken
        };
        apiParam.sign = signURL(apiParam, key);

        log.warn("param: {}, sign: {}", apiParam, apiParam.sign)
        let result = invoker.invokeWithoutIntercept("RefreshToken", apiParam);
        let response = result.result.pop_auth_token_refresh_response;
        if (!isValue(response)) return null;
        let config = {};
        if (!isValue(response.access_token)){
            log.warn("Can not refresh access token, access_token is empty.");
            return null;
        }
        config.access_token = response.access_token;
        if (isValue(response.refresh_token)) {
            config.refresh_token = response.refresh_token;
        }
        return config;
    }
    return null;
}

