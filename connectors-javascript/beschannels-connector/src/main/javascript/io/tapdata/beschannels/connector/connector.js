var invoker = loadAPI();

function discoverSchema(connectionConfig) {
    return ['致趣_微信注册用户打标签'];
}

function connectionTest(connectionConfig) {
    let dataCode = 0;
    try {
        let httpResult = invoker.invokeWithoutIntercept("SetUserTag",{"tags":"","key_openid":"","tag_time":""});
        if (!('undefined' === httpResult.httpCode || null == httpResult.httpCode || 200 < httpResult.httpCode || httpResult.httpCode >= 300))
            if (!('undefined' === httpResult.result || null == httpResult.result)) dataCode = httpResult.result.code;
        dataCode = (undefined === dataCode || null == dataCode) ? '0' : dataCode;
    }catch (e){
        dataCode = 0;
    }
    let dataMsg = '';
    if (41009 == dataCode){
        dataMsg = "Successfully, Fill in information that meets API usage requirements.";
    }else if (40002 == dataCode){
        dataMsg = "Error AppID of WeChat official account.";
    }else if (-40001 == dataCode){
        dataMsg = "Error Customer Unique Encrypted or Customer unique identification code.";
    }else {
        dataMsg = " The information filled in is incorrect and does not meet the API usage requirements. Please try again or fill in again ";
    }
    return [{"test": "Test API", "code": (41009 == dataCode) ? 1 : -1, "result": dataMsg}];
}

function insertRecord(connectionConfig, nodeConfig, eventDataMap) {
    let record = eventDataMap.afterData;
    if ('undefined' === record || null == record) {
        log.info("Record is empty, it is ignore this record now.");
        return false;
    }
    let openid = record[nodeConfig.weChatOpenId];//手机号（多个openid以逗号隔开，最多100个openid）
    if ("undefined" === openid || null == openid) {
        log.info("Can not tag for user whois open ID is empty, it is ignore this now.");
        return false;
    }
    if ("" !== openid) {
        //判断是否来源微信扫码注册
        let registerFrom = record[nodeConfig.weChatOpenIdFrom];
        if ("undefined" !== registerFrom && null != registerFrom) {
            if (registerFrom == "social:wechatmp-qrcode") {
                let sendData = {"key_openid": openid, "tag_time": new Date().getTime()}
                let httpResult = invoker.invoke("SetUserTag", sendData);
                let httpCode = httpResult.httpCode;
                if ('undefined' === httpCode || null == httpCode || 200 < httpCode || httpCode >= 300) {
                    log.warn("Request failed, http code error. can not tag for user whois open ID is {}, http code is {}.", openid, httpCode)
                    return false;
                }
                let httpData = httpResult.result;
                if ('undefined' === httpData || null == httpData) {
                    log.warn("Request failed, http data is empty. can not tag for user whois open ID is {}.", openid)
                    return false;
                }
                let dataCode = httpData.code;
                if ("undefined" === dataCode || null == dataCode) {
                    log.warn("Request failed, data code is empty. can not tag for user whois open ID is {}.", openid)
                    return false;
                }
                let dataMsg = httpData.message;
                if (undefined === dataMsg || null == dataMsg) dataMsg = 'unknown'
                if ("0" == dataCode){
                    return true;
                }else {
                    log.warn("Request failed, can not tag for user whois open is is {}, the reason is {}.", openid, dataMsg)
                    return false;
                }
            } else {
                log.warn("Request failed, http code error. can not tag for user {} whois register from {} is not support, only support wechat which code is 'social:wechatmp-qrcode'.", openid, registerFrom)
                return false;
            }
        } else {
            log.info("Request failed, http code error. can not tag for user {} whois register from is unknown.", openid)
            return false;
        }
    } else {
        log.info("Request failed, http code error. can not tag for user whois open ID is empty.")
        return false;
    }
}

function updateToken(){

}

