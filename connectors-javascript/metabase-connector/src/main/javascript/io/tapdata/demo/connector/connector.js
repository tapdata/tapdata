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
    return str.replaceAll(/\^|\.|\s+|\*|\?|\!|\/|\\|\$|\—+|\@|\%|\*|\~|\;|\:|\'|\"|\#|\&|\||\，|\,|\。|\`|\！|\[|\]|\？|\{|\}|\(|\)|\（|\）|\＜|\＞|\<|\>|\≤|\≥|\《|\》|\-|\+|\=/g, "");
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
            log.warn("{} read {} :{}", thisCard.name,  temp.status, temp.error);
            return;
        }
    } catch (e) {
        throw ("Failed to query data. Please check whether the data can be queried properly. " + e.message)
        return;
    }
}

function connectionTest(connectionConfig) {
    try {
        let sessionToken = invoker.invoke('TAP_GET_TOKEN session api');
        if (sessionToken.result === 'undefined'
            || sessionToken.result === null
            || sessionToken.result.id === null
            || sessionToken.result.id === 'undefined'
            || sessionToken.result.errors != null) {
            return [{
                "test": "Please check whether the account or secret or HTTP address is correct. ",
                "code": -1,
                "result": "Please check whether the account or secret or HTTP address is correct. "
            }];
        }
        let invoke = invoker.invoke('TAP_TABLE[allCard](PAGE_NONE)allCard',
            {"sessionToken": sessionToken.result.id});
        let nameItem = invoke.result;
        let idItem = invoke.result[0].id;
        if (nameItem === 'undefined'
            || nameItem === null
            || idItem === null
            || idItem === 'undefined') {
            return [{
                "test": " Please check whether the account or secret or HTTP address is correct",
                "code": -1,
                "result": "Please check whether the HTTP connection is correct. "
            }];
        }
        return [{
            "test": " Check the account read database permission.",
            "code": invoke ? 1 : -1,
            "result": invoke ? "Pass" : "Please check whether the account or secret or HTTP address is correct. "
        }];
    } catch (e) {
        return [{
            "test": " Check the account, password, and HTTP address",
            "code": -1,
            "result": "Please check whether the HTTP connection is correct. "
        }];
    }
}

