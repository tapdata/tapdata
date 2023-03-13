config.setStreamReadIntervalSeconds(3);
var afterData;
var nodes = {};

function discoverSchema(connectionConfig) {
    return ['Leads', 'Contacts', 'Opportunity'];
}
function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    let invoke;
    let isFirst = true;
    do {
        if (!isAlive()) break;
        if (isFirst) {
           invoke =  invoker.invoke(getApiName(tableName), {"first": 2000}).result.data.uiapi.query[tableName+""];
            afterData = invoke.pageInfo.endCursor;
        } else {
           invoke = invoker.invoke(getApiName(tableName), {"first": 2000,"after": afterData}).result.data.uiapi.query[tableName+""];
        }
        let resultData = invoke.edges;
        for (let j = 0; j < resultData.length; j++) {
            let newNode = {};
            let n = resultData[j].node
            let keys = Object.keys(n);
            for (let i = 0; i <= keys.length; i++) {
                let key = keys[i];
                let value = resultData[key];
                if (value instanceof Map || typeof (value) === 'object') {
                    value = value["value"];
                }
                newNode[key] = value;
            }
            nodes.push(newNode)
        }
        isFirst = false;
        batchReadSender.send(nodes, tableName, {}, false);
        nodes = {};
    } while (invoke.pageInfo.hasNextPage);
}

function streamRead(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {
    var arr = [];
    if (!checkParam(tableNameList)) return [];
    for(let tableName in tableNameList) {
        var date = offset[tableName];
        let invoke = invoker.invoke(getApiName(tableName) + "stream read", {"first": 2000}).result.data.uiapi.query[tableName + ""];
        let edges = invoke.edges;
        if (checkParam(edges)) {
            for (let j = 0; j < edges.length; j++) {
                let newNode = {};
                let n = edges[j].node;

                let lastModifiedDate = new Date(n.LastModifiedDate.value);
                let createdDate = new Date(n.CreatedDate.value);
                log.warn("createdDate:{}", createdDate);
                log.warn("lastModifiedDate:{}", lastModifiedDate);

                if (n.LastModifiedDate.value === n.CreatedDate.value) {
                    arr[j] = {
                        "eventType": "i",
                        "tableName": tableName,
                        "afterData": sendData(n, newNode)
                    };
                } else {
                    arr[j] = {
                        "eventType": "u",
                        "tableName": tableName,
                        "afterData": sendData(n, newNode)
                    };
                }
            }
            offset[tableName] = dateUtils.nowDate();
            streamReadSender.send(arr, tableName, offset);
        } else {
            return;
        }
    }
}
function connectionTest(connectionConfig) {
    let sessionToken = invoker.invoke('TAP_GET_TOKEN session api');
    let invoke = invoker.invoke('TAP_TABLE[allCard](PAGE_NONE)allCard',
        {"sessionToken": sessionToken.result.id});
    return [{
        "test": " Check the account read database permission.",
        "code": invoke ? 1 : -1,
        "result": invoke ? "Pass" : "Not pass"
    }];
}

