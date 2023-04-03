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

function disassemblyData(resultData, result) {
    for (let j = 0; j < resultData.length; j++) {
        if (!isAlive()) break;
        let resultItem = resultData[j];
        let nod = resultItem.node;
        let keys = Object.keys(nod);
        for (let index = 0; index < keys.length; index++) {
            if (!isAlive()) break;
            let key = keys[index];
            if (typeof (nod[key].value) == "boolean" || nod[key].value) nod[key] = nod[key].value;
        }
        result.push(nod);
    }
}

function sendData(nod) {
    let keys = Object.keys(nod);
    for (let index = 0; index < keys.length; index++) {
        if (!isAlive()) break;
        let key = keys[index];
        if (typeof (nod[key].value) == "boolean" || nod[key].value) nod[key] = nod[key].value;
    }
    return nod;
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