var batchStart = dateUtils.nowDate();
function discoverSchema(connectionConfig, schemaSender) {
    let sessionToken;
    let invoke;
    try {
        sessionToken = invoker.setTimeOut(10000).invoke('TAP_GET_TOKEN session api');
    } catch (e) {
        log.warn("Metabase's discover_schema method fails to call the sessionToken api: " + e);
        return;
    }
    try {
        invoke = invoker.setTimeOut(10000).invoke('TAP_TABLE[allCard](PAGE_NONE)allCard',
            {"sessionToken": sessionToken.result.id});
    } catch (e) {
        log.warn("Metabase's discover_schema method fails to call the allCard api: " + e);
        return;
    }
    let tableList = [];
    for (let index = 0; index < invoke.result.length; index++) {
        let invokeCard;
        try {
            invokeCard = invoker.setTimeOut(10000).invoke(
                'TAP_TABLE[queryExportFormat](PAGE_NONE:data)queryExportFormat',
                {"card-id": parseInt(invoke.result[index].id), "sessionToken": sessionToken.result.id});
        } catch (e) {
            log.warn("Metabase's discover_schema method fails to call the queryExportFormat api: " + e);
            continue;
        }
        let clos = invokeCard.result.data.cols;
        let name = clearSpecial(invoke.result[index].name);
        let table = new Map();
        table.set("name", "Card_" + invoke.result[index].id + "_" + name + "");
        let fields = new Map();
        for (let j = 0; j < clos.length; j++) {
            let filedEnt = clos[j];
            let keyName = filedEnt.display_name;
            let keyType = filedEnt.base_type;
            let keyProps = new Map();
            if (keyType === 'type/Text') {
                keyProps.set("type", "String");
            } else if (keyType === 'type/Float') {
                keyProps.set("type", "Float");
            } else if (keyType === 'type/Array') {
                keyProps.set("type", "Array");
            } else if (keyType === 'type/boolean') {
                keyProps.set("type", "Boolean");
            } else if (keyType === 'type/Instant') {
                keyProps.set("type", "Date");
            } else if (keyType === 'type/Double') {
                keyProps.set("type", "Number");
            } else if (keyType === 'type/BigInteger') {
                keyProps.set("type", "Long");
            } else if (keyType === 'type/Integer') {
                keyProps.set("type", "Number");
            } else {
                keyProps.set("type", "Object");
            }
            fields.set(keyName, keyProps);
        }
        table.set("fields", fields);
        tableList.push(table);
        if (index !== 0 && index % 20 === 0) {
            schemaSender.send(tableList);
            tableList = [];
        }
        sleeped(1000);
    }
    if (tableList.length > 0) {
        schemaSender.send(tableList);
    }
}

function clearSpecial(str) {
    return str.replaceAll(/\^|\.|\s+|\*|\?|\!|\/|\\|\$|\—+|\@|\%|\*|\~|\;|\:|\'|\"|\#|\&|\||\，|\,|\。|\`|\！|\[|\]|\？|\{|\}|\(|\)|\（|\）|\＜|\＞|\<|\>|\≤|\≥|\《|\》|\-|\+|\=/g, "");
}

function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    let sessionToken = invoker.invoke('TAP_GET_TOKEN session api', {});
    let data = invoker.invoke('TAP_TABLE[allCard](PAGE_NONE)allCard',
        {"sessionToken": sessionToken.result.id});
    let id = tableName.split("_")[1];
    let thisCard = {};
    for (let index = 0; index < data.result.length; index++) {
        if (data.result[index].id == id) {
            thisCard = data.result[index];
            break;
        }
    }
    let invoke = invoker.invoke(
        'TAP_TABLE[queryExportFormat](PAGE_NONE:data)queryExportFormat',
        {"card-id": parseInt(id),"sessionToken": sessionToken.result.id});
    let resut = [];
    let temp = invoke.result;
    for (let index = 0; index < temp.length; index++) {
        log.warn(typeof temp[index]);
        temp[index]['Question_Name'] = thisCard.name;
        temp[index]['Question_ID'] = thisCard.id;
        temp[index]['Current_Date'] = dateUtils.nowDate();
        resut.push(temp[index]);
    }
    batchReadSender.send(resut, {}, false);
}

function connectionTest(connectionConfig) {
    let sessionToken = invoker.invoke('TAP_GET_TOKEN session api');
    let invoke = invoker.invoke('TAP_TABLE[allCard](PAGE_NONE)allCard',
        {"sessionToken": sessionToken.result.id});
    return [{"test": " Check the account read database permission.", "code": invoke ? 1 : -1, "result": invoke ? "Pass" : "Not pass"}];
}

