var batchStart = nowDate();

function discover_schema(connectionConfig, schemaSender) {
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

function batch_read(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    let sessionToken;
    try {
        sessionToken = invoker.setTimeOut(10000).invoke('TAP_GET_TOKEN session api', {});
    } catch (e) {
        log.warn("Metabase's batch_read method fails to call the sessionToken api: " + e);
    }
    let data
    try {
        data = invoker.setTimeOut(10000).invoke('TAP_TABLE[allCard](PAGE_NONE)allCard',
            {"sessionToken": sessionToken.result.id});
    } catch (e) {
        log.warn("Metabase's batch_read method fails to call the allCard api: " + e);
    }

    let id = tableName.split("_")[1];
    let thisCard = {};
    for (let index = 0; index < data.result.length; index++) {
        if (data.result[index].id == id) {
            thisCard = data.result[index];
            break;
        }
    }
    let invoke;
    try {
        invoke = invoker.setTimeOut(10000).invoke(
            'TAP_TABLE[queryExportFormat](PAGE_NONE:data)queryExportFormat',
            {"card-id": parseInt(id), "sessionToken": sessionToken.result.id});
    } catch (e) {
        log.warn("Metabase's batch_read method fails to call the queryExportFormat api: " + e);
    }

    let result = [];
    let rows = invoke.result.data.rows;
    let clos = invoke.result.data.cols;
    for (let index = 0; index < rows.length; index++) {
        let tempData = rows[index];
        let temp = new Map();
        for (let j = 0; j < tempData.length; j++) {
            temp.set(clos[j].display_name, tempData[j]);
        }
        temp.set('Question_Name', thisCard.name);
        temp.set('Question_ID', thisCard.id);
        temp.set('Current_Date', nowDate());
        result.push(temp);
    }
    batchReadSender.send(result, {}, false);
}

function connection_test(connectionConfig) {
    let sessionToken = invoker.invoke('TAP_GET_TOKEN session api');
    let invoke = invoker.invoke('TAP_TABLE[allCard](PAGE_NONE)allCard',
        {"sessionToken": sessionToken.result.id});
    return [{
        "TEST": " Check the account read database permission.",
        "CODE": invoke ? 1 : -1,
        "RESULT": invoke ? "Pass" : "Not pass"
    }];
}
