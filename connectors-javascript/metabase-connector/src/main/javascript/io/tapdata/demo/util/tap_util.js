/**
 *  This is the toolkit encapsulated by Tap Data.
 * */
var invoker = loadAPI().httpConfigAsGlobal({'timeout': 30000});
var OptionalUtil = {
    isEmpty: function (obj) {
        return typeof (obj) == 'undefined' || null == obj;
    },
    notEmpty: function (obj) {
        return !this.isEmpty(obj);
    }
}

function iterateAllData(apiName, offset, call) {
    if (OptionalUtil.isEmpty(apiName)) {
        log.error("Please specify the corresponding paging API name or URL .");
    }
    let res;
    let error;
    do {
        let response = invoker.invoke(apiName, offset);
        res = response.result;
        error = response.error;
    } while (call(res, offset, error));
}

function commandAndConvertData(apiName, params, call) {
    if (OptionalUtil.isEmpty(apiName)) {
        log.error("Please specify the corresponding paging API name or URL .");
    }
    let invokerData = invoker.invoke(apiName, params);
    return call(invokerData.result);
}

function checkParam(param) {
    return 'undefined' !== param && null != param;
}

function result(invoke, httpCode) {
    log.warn("invoke3:{}",invoke)
    if (httpCode >= 200 && httpCode < 300) {
        return "Pass";
    } else if (invoke.result && invoke.result.errors && invoke.result.errors.username) {
        return "Error:" + invoke.result.errors.username + " Please check whether the account or secret. " + "HttpCode: " + invoke.httpCode
    } else if (invoke.result && invoke.result.errors && invoke.result.errors.password) {
        return "Error:" + invoke.result.errors.password + " Please check whether the account or secret. " + "HttpCode: " + invoke.httpCode
    } else if (invoke.result && invoke.result === "Unauthenticated") {
        return "Error: " + invoke.result + " HttpCode: " + httpCode;
    } else if (invoke.result === 'undefined' || invoke.result === null || invoke.result[0].id === null || invoke.result[0].id === 'undefined') {
        return "Error: Please check whether the HTTP connection is correct.";
    } else {
        return "Error: Unknown. HttpCode: " + httpCode
    }
}
