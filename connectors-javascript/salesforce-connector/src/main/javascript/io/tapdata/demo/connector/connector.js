var batchStart = dateUtils.nowDate();
var afterData;
function discoverSchema(connectionConfig) {
    return ['Leads', 'Contacts', 'Opportunity'];
}


function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    let invoke;
    let first = true;
    let nodes = {};
    let result = {};
    do {
        if (!isAlive()) break;
        if (tableName === 'Leads' && first) {
           invoke =  invoker.invoke(getApiName(tableName), {"first": 2000}).result.data.uiapi.query.Contact;
            afterData = invoke.pageInfo.endCursor;
        } else {
           invoke = invoker.invoke(getApiName(tableName), {"first": 2000,"after": afterData}).result.data.uiapi.query.Contact;
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
        result.push(nodes)
        first = false;
        batchReadSender.send(result, tableName, {}, false);
    } while (invoke.pageInfo.hasNextPage);
}

function streamRead(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {
    if (!isParam(offset) || null == offset || typeof (offset) != 'object') offset = {};

    streamReadSender.send(offset);
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

