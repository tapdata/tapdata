/**
 *  This is the toolkit encapsulated by Tap Data.
 * */
var invoker = loadAPI();
var OptionalUtil = {
    isEmpty: function (obj) {
        return typeof (obj) == 'undefined' || null == obj;
    },
    notEmpty: function (obj) {
        return !this.isEmpty(obj);
    }
}

function iterateAllData(apiName, offset, params, call) {
    if (OptionalUtil.isEmpty(apiName)) {
        log.error("Please specify the corresponding paging API name or URL .");
    }
    let res;
    let error;
    do {
        let response = invoker.invoke(apiName, params);
        res = response.result;
    } while (call(res, offset, error));
}

function commandAndConvertData(apiName, params, call){
    if (OptionalUtil.isEmpty(apiName)) {
        log.error("Please specify the corresponding paging API name or URL .");
    }
    let invokerData = invoker.invoke(apiName, params);
    return call(invokerData.result);
}

function result(invoke, httpCode) {
    if (httpCode >= 200 && httpCode < 300) {
        return "Pass";
    } else if (invoke.result[0] && invoke.result[0].errorCode && invoke.result[0].message) {
        return "Error: " + invoke.result[0].message + " Code: " + invoke.result[0].errorCode + " HttpCode: " + httpCode ;
    } else {
        return "Error: Unknown. HttpCode: " + httpCode
    }
}