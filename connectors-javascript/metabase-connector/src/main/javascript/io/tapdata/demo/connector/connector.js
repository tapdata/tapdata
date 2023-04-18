var batchStart = dateUtils.nowDate();
function discoverSchema(connectionConfig) {
    try {
        let sessionToken = invoker.invoke('TAP_GET_TOKEN session api');
        if ('undefined' === sessionToken.result || sessionToken.result === null) {
            log.error("Please check whether the account or secret or HTTP address is correct. ");
            return [];
        }
        let invoke = invoker.invoke('TAP_TABLE[allCard](PAGE_NONE)allCard',
            {"sessionToken": sessionToken.result.id});
        if ('undefined' === invoke.result || invoke.result === null || invoke.result.length === 0) {
            log.error("Please check whether the account or secret or HTTP address is correct ");
            return [];
        }
        let tableList = [];
        for (let index = 0; index < invoke.result.length; index++) {
            let nameItem = invoke.result[index].name;
            let idItem = invoke.result[index].id;
            if (nameItem === 'undefined' || nameItem === null || idItem === 'undefined' || idItem === null) {
                log.warn("This table is empty");
                continue;
            }
            tableList.push("Card_" + idItem + "_" + clearSpecial(nameItem) + "");
        }
        return tableList;
    } catch (e) {
        log.error(e.message);
    }
}

function clearSpecial(str) {
    let pattern = new RegExp('[^\\w\\s\\u4e00-\\u9fa5]', 'g');
    let matches = str.match(pattern);
    return str.replaceAll(matches, '');
}

function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    try {
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
            {"card-id": parseInt(id), "sessionToken": sessionToken.result.id});
        let result = [];
        let temp = invoke.result;
        if (checkParam(temp[0])) {
            for (let index = 0; index < temp.length; index++) {
                temp[index]['Question_Name'] = thisCard.name;
                temp[index]['Question_ID'] = thisCard.id;
                temp[index]['Current_Date'] = dateUtils.nowDate();
                result.push(temp[index]);
            }
            batchReadSender.send(result, tableName, offset);
        } else {
            log.warn("{} read {} :{}", thisCard.name, temp.status, temp.error);
            return;
        }
    } catch (e) {
        throw ("Failed to query data. Please check whether the data can be queried properly. " + e.message)
        return;
    }
}

function connectionTest(connectionConfig) {
    let HttpCode;
    let res = [];
    try {
        let sessionToken = invoker.invoke('TAP_GET_TOKEN session api');
        HttpCode = sessionToken.httpCode;
        res.push({
            "test": "Check whether the account or secret or HTTP address is correct. ",
            "code": exceptionUtil.statusCode(HttpCode),
            "result": result(sessionToken, HttpCode)
        })
        if (exceptionUtil.statusCode(HttpCode) === -1) {
            return res;
        }
        let invoke = invoker.invoke('TAP_TABLE[allCard](PAGE_NONE)allCard',
            {"sessionToken": sessionToken.result.id});
        HttpCode = invoke.httpCode;
        res.push({
            "test": " Check the account read database permission.",
            "code": exceptionUtil.statusCode(HttpCode),
            "result": result(invoke, HttpCode)
        })
        return res;
    } catch (e) {
        return [{
            "test": " Check whether the account or secret or HTTP address is correct.",
            "code": -1,
            "result": exceptionUtil.eMessage(e)
        }];
    }
}

