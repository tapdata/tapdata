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

function disassemblyData(resultData, result, offset) {
    for (let j = 0; j < resultData.length; j++) {
        if (!isAlive()) break;
        let resultItem = resultData[j];
        let nod = resultItem.node;
        let keys = Object.keys(nod);
        offset = {
            "currentTime" : Number(new Date(nod.CreatedDate.value)),
            "recordId" : nod.Id
        }
        for (let index = 0; index < keys.length; index++) {
            if (!isAlive()) break;
            let key = keys[index];
            if (typeof (nod[key].value) == "boolean" || nod[key].value) nod[key] = nod[key].value;
        }
        result.push(nod);
    }
}

function disassemblyDataAfter(resultData, result, offset) {
    let resultItem = resultData[j];
    let nod = resultItem.node;
    let keys = Object.keys(nod);
    for (let j = 0; j < resultData.length; j++) {
        if (Number(new Date(nod.CreatedDate.value) < offset.currentTime) || nod.Id === offset.recordId) {
            continue;
        }
        if (!isAlive()) break;
        offset = {
            "currentTime" : Number(new Date(nod.CreatedDate.value)),
            "recordId" : nod.Id
        }
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

function streamData(index,invoke, pageInfo, afterData, tableNameList, offset, arr, pageSize, streamReadSender,first) {
    log.warn("zhelizheliu")
    log.warn("invoke.result.data.uiapi:{}",invoke.result.data.uiapi)
    log.warn("tablename:{}",tableNameList)
    log.warn("invoke.result.data.uiapi.query[tableNameList[index] :{}",invoke.result.data.uiapi.query[tableNameList[index] + ""])
    let resultMap = invoke.result.data.uiapi.query[tableNameList[index] + ""];
    log.warn("这里666")

    let resultData = resultMap.edges;
    if (resultData[0] && resultData.length > 0) {
        afterData = resultMap.pageInfo.endCursor;
        for (let j = 0; j < resultData.length; j++) {
            if (!isAlive()) break;
            let resultItem = resultData[j];
            let nod = resultItem.node;
            let cId = offset.recordId
            log.warn("这里1")
            offset = {
                "currentTime" : Number(new Date(new Date().getTime())),
                "recordId" : nod.Id
            }
            log.warn("这里2")
            if (Number(new Date(nod.LastModifiedDate.value)) > isStreamFirst(offset,first) || nod.Id !== cId) {
                if (nod.LastModifiedDate.value === nod.CreatedDate.value) {
                    arr[j] = {
                        "eventType": "i",
                        "tableName": tableNameList[index],
                        "afterData": sendData(nod),
                        "referenceTime": Number(new Date())
                    };
                } else {
                    arr[j] = {
                        "eventType": "u",
                        "tableName": tableNameList[index],
                        "afterData": sendData(nod),
                        "referenceTime": Number(new Date())
                    };
                }
                log.warn("offset.currentTime:{}",offset.currentTime)
                log.warn("这里3")
            } else {

                offset = {"currentTime" : Number(new Date(new Date().getTime()))}
                break;
            }
        }
        streamReadSender.send(arr, tableNameList[index], offset);
        arr = [];
    }
}

function isStreamFirst(offset,first) {
    if (first) {
        return offset;
    } else {
        return offset.currentTime;
    }

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