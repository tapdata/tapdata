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

function disassemblyData(resultData, result, offset, pageInfo) {
    for (let j = 0; j < resultData.length; j++) {
        if (!isAlive()) break;
        let resultItem = resultData[j];
        let nod = resultItem.node;
        let keys = Object.keys(nod);
        if (resultItem.cursor === pageInfo.endCursor) {
            let time = Number(new Date(nod.CreatedDate.value));
            if (!offset || !offset.currentTime || time > offset.currentTime) {
                offset = {
                    "currentTime": time,
                    "recordId": nod.Id,
                    "firstTime": false
                }
            }
        }
        for (let index = 0; index < keys.length; index++) {
            if (!isAlive()) break;
            let key = keys[index];
            if (typeof (nod[key].value) == "boolean" || nod[key].value) nod[key] = nod[key].value;
        }
        result.push(nod);
    }
    return offset;
}

function disassemblyDataAfter(resultData, result, offset, pageInfo) {
    let bool = false;
    for (let j = 0; j < resultData.length; j++) {
        let resultItem = resultData[j];
        let nod = resultItem.node;
        let keys = Object.keys(nod);
        let createTime = Number(new Date(nod.CreatedDate.value));
        if (offset && offset.currentTime) {
            if (createTime < offset.currentTime) {
                if (!bool) {
                    if (!offset.recordId) {
                        bool = true;
                    } else {
                        if (nod.id === offset.recordId) {
                            bool = true;
                        }
                        continue;
                    }
                }
            }
        }
        if (!isAlive()) break;
        if (resultItem.cursor === pageInfo.endCursor) {
            let time = new Date(nod.CreatedDate.value).getTime();
            if (!offset || !offset.currentTime || time > offset.currentTime) {
                offset = {
                    "currentTime": time,
                    "recordId": nod.Id,
                    "firstTime": false
                }
            }
        }
        for (let index = 0; index < keys.length; index++) {
            if (!isAlive()) break;
            let key = keys[index];
            if (typeof (nod[key].value) == "boolean" || nod[key].value) nod[key] = nod[key].value;
        }
        result.push(nod);
    }
    return offset;
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

function streamData(index, invoke, resultMap, pageInfo, tableNameList, offset, arr, pageSize, streamReadSender) {
    resultMap = invoke.result.data.uiapi.query[tableNameList[index] + ""];
    let resultData = resultMap.edges;
    if (resultData[0] && resultData.length > 0) {
        for (let j = 0; j < resultData.length; j++) {
            if (!isAlive()) break;
            let resultItem = resultData[j];
            let nod = resultItem.node;
            let lastModifiedDate = Number(new Date(nod.LastModifiedDate.value));
            if (lastModifiedDate >= offset.currentTime) {
                log.warn("lastModifiedDate:{}", lastModifiedDate)
                if (offset.recordId !== nod.id) {
                    offset.currentTime = BigInt(lastModifiedDate);
                    offset.recordId = nod.Id;
                    offset.firstTime = false;
                    if (nod.LastModifiedDate.value === nod.CreatedDate.value) {
                        log.warn("增加勒一条数据")
                        arr[j] = {
                            "eventType": "i",
                            "tableName": tableNameList[index],
                            "afterData": sendData(nod),
                            "referenceTime": Number(new Date())
                        };
                    } else {
                        log.warn("修改勒一条数据")
                        arr[j] = {
                            "eventType": "u",
                            "tableName": tableNameList[index],
                            "afterData": sendData(nod),
                            "referenceTime": Number(new Date())
                        };
                    }
                    log.warn("offset.currentTime:{}", offset.currentTime)
                } else {
                    continue;
                    log.warn("continue")
                }
            }
        }
        streamReadSender.send(arr, tableNameList[index], offset);
        arr = [];
    }
}

function checkAuthority(invoke, httpCode) {
    if (httpCode === 400 || (invoke.result.error_description && invoke.result.error_description === "expired access/refresh token")) {
        throw ("Error: " + invoke.result.error_description + " HttpCode: " + httpCode);
    } else if (invoke.result[0] && invoke.result[0].errorCode && invoke.result[0].message) {
        throw "Error: " + invoke.result[0].message + " Code: " + invoke.result[0].errorCode + " HttpCode: " + httpCode;
    }
}

function result(invoke, httpCode) {
    if (httpCode >= 200 && httpCode < 300) {
        return "Pass";
    } else if (invoke.result[0] && invoke.result[0].errorCode && invoke.result[0].message) {
        return "Error: " + invoke.result[0].message + " Code: " + invoke.result[0].errorCode + " HttpCode: " + httpCode;
    } else {
        return "Error: Unknown. HttpCode: " + httpCode
    }
}