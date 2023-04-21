/**
 *  This is the toolkit encapsulated by Tap Data.
 * */
var invoker = loadAPI().httpConfigAsGlobal({"timeout":20000});
var OptionalUtil = {
    isEmpty: function (obj) {
        return typeof (obj) == 'undefined' || null == obj;
    },
    notEmpty: function (obj) {
        return !this.isEmpty(obj);
    }
}

function checkParam(param) {
    return 'undefined' !== param && null != param;
}

function iterateAllData(apiName, offset, call) {
    if (OptionalUtil.isEmpty(apiName)) {
        log.error("Please specify the corresponding paging API name or URL .");
    }
    let res;
    do {
        let response = invoker.invoke(apiName, offset);
        res = response.result;
    } while (call(res, offset));
}

function commandAndConvertData(apiName, params, call){
    if (OptionalUtil.isEmpty(apiName)) {
        log.error("Please specify the corresponding paging API name or URL .");
    }
    let invokerData = invoker.invoke(apiName, params);
    return call(invokerData.result);
}

function checkAuthority(invoke, httpCode) {
    if (httpCode === 400 || (invoke.result.error_description && invoke.result.error_description === "expired access/refresh token")){
        throw ("Error: " + invoke.result.error_description + " HttpCode: " + httpCode );
    }else if (invoke.result[0] && invoke.result[0].errorCode && invoke.result[0].message) {
        throw "Error: " + invoke.result[0].message + " Code: " + invoke.result[0].errorCode + " HttpCode: " + httpCode ;
    }
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