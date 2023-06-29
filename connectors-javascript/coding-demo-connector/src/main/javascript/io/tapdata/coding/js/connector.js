var batchStart = dateUtils.nowDate();

function discoverSchema(connectionConfig, schemaSender) {
    //schemaSender.send(['ACC']);
    schemaSender.send([{
        "name": 'Issues1',
        "fields": {
            "Code": {
                "type": "Number",
                "isPrimaryKey": true,
                "primaryKeyPos": 1
            }
        }
    }]);
}

function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    if (!isParam(offset) || null == offset || typeof (offset) != 'object' || 'undefined' === offset.pageNumber || null === offset.pageNumber)
        offset = {
        "pageNumber": 1,
        "pageSize": pageSize,
        "sortKey": 'CREATED_AT',
        "sortValue": 'ASC',
        "conditions": [{"Key": 'CREATED_AT', "Value": '1000-01-01_' + batchStart}]
    };
    read('Issues', offset, batchReadSender, false);
}

function streamRead(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {
    if (!isParam(offset) || null == offset || typeof (offset) != 'object' || Object.keys(offset).length <= 0) offset = {
        "pageNumber": 1,
        "pageSize": pageSize,
        "sortKey": 'UPDATED_AT',
        "sortValue": 'ASC',
        "conditions": [{"Key": 'UPDATED_AT', "Value": batchStart + '_' + dateUtils.nowDate()}]
    };
    let condition = arrayUtils.firstElement(offset.Conditions);
    offset.Conditions = [{
        Key: "UPDATED_AT",
        Value: isParam(condition) && null != condition ? arrayUtils.firstElement(condition.Value.split('_')) + '_' + dateUtils.nowDate() : batchStart + '_' + dateUtils.nowDate()
    }];
    offset.sortKey = "UPDATED_AT";
    read('Issues', offset, streamReadSender, true);
}

function connectionTest(connectionConfig) {
    let issuesList = invoker.invoke('Issues',{
        "SortKey": 'UPDATED_AT',
        "Conditions": [{"Key": 'UPDATED_AT', "Value": batchStart + '_' + dateUtils.nowDate()}],
        "SortValue": 'ASC'});
    let items = [{
        "test": " Check whether the interface call passes of Issues List API.",
        "code": issuesList ? 1 : -1,
        "result": issuesList ? "Pass" : "Not pass"
    }];
    if (issuesList) {
        items.push({
            "test": "Read log",
            "code": 1,
            "result": "Pass"
        });
    }
    return items;
}

function read(urlName, offset, sender, isStreamRead) {
    iterateAllData(urlName, offset, (result, offsetNext) => {
        try {
            if (isStreamRead && isParam(result.Response.Data.List) && result.Response.Data.TotalPage < result.Response.Data.PageNumber)
                offset.conditions = [{
                    "Key": isStreamRead ? 'UPDATED_AT' : "CREATE_AT",
                    "Value": dateUtils.formatDate(result.Response.Data.List[result.Response.Data.List.length - 1].UpdatedAt) + '_' + dateUtils.nowDate()
                }];
            sender.send(result.Response.Data.List, "Issues1", offset);
            offsetNext.pageNumber = (!isStreamRead || (isStreamRead && result.Response.Data.TotalPage >= result.Response.Data.PageNumber)) ? parseInt(""+(offsetNext.pageNumber + 1)) : 1;
            return offsetNext.pageNumber <= result.Response.Data.TotalPage && isAlive();
        } catch (e) {
            throw e + "\n Http response is: " + JSON.stringify(result);
        }
    });
}

function commandCallback(connectionConfig, nodeConfig, commandInfo) {
    invoker.addConfig(connectionConfig, nodeConfig);
    let userInfo = (invoker.httpConfig({"timeout": 3000}).invoke('MyUserInfo')).result;
    if (commandInfo.command === 'DescribeUserProjects') {
        let data = invoker.invoke("DescribeUserProjects", {userId: userInfo.Response.User.Id});
        return {
            "items": arrayUtils.convertList(data.result.Response.ProjectList, {'DisplayName': 'label', 'Name': 'value'}),
            "page": 1,
            "size": data.result.Response.ProjectList.length,
            "total": data.result.Response.ProjectList.length
        };
    }
}
