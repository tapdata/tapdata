config.setStreamReadIntervalSeconds(60);
var batchStart = Date.parse(new Date());
var afterData;
var clientInfo = {
    "client_id": "3MVG9n_HvETGhr3Brv_TokDiPcVsfa0TubCszRjXdeqnY0z7cBUmaW0I9eTZtnz0oIC4zMLOxcUaCiEMpwr57",
    "client_secret": "3ACD5F530CE58AFA0A5224D2CD04A0A97D2495D60575E4592B00348D8D0C55BA",
    "_endpoint": "https://163com-8a-dev-ed.develop.my.salesforce.com",
    "url": "https://login.salesforce.com",
    "version": "57.0"
}

function discoverSchema(connectionConfig) {
    return [{
        "name": 'contact',
        "fields": {
            'Id':{
                'type':'String',
                'comment':'',
                'nullable':false,
                'isPrimaryKey':true,
                'primaryKeyPos':1
            },
            'AccountId':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'AssistantName':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'AssistantPhone':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'Birthdate':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'CleanStatus':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'Department':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'Description':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'Email':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'EmailBouncedDate':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'EmailBouncedReason':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'Fax':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'FirstName':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'HomePhone':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'IndividualId':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'IsDeleted':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'IsEmailBounced':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'Jigsaw':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'Languages__c':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'LastActivityDate':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'LastName':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'LastReferencedDate':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'LastViewedDate':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'LeadSource':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'MailingCity':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'MailingCountry':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'MailingGeocodeAccuracy':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'MailingLatitude':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'MailingLongitude':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'MailingPostalCode':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'MailingState':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'MailingStreet':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'MasterRecordId':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'MobilePhone':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'Name':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'OtherCity':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'OtherCountry':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'OtherGeocodeAccuracy':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'OtherLatitude':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'OtherLongitude':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'OtherPhone':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'OtherPostalCode':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'OtherState':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'OtherStreet':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'OwnerId':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'Phone':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'PhotoUrl':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'RecordTypeId':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'ReportsToId':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'Salutation':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'Title':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'LastModifiedById':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'LastModifiedDate':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'CreatedDate':{
                'type':'String',
                'comment':'',
                'nullable':false
            }
        }
    }];
}

function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    let invoke;
    let result = [];
    let isFirst = true;
    let pageInfo = {"hasNextPage": true};
    do {
        let uiApi;
        if (isFirst) {
            clientInfo.Authorization = connectionConfig.access_token;
            invoke = invoker.invoke(getApiName(tableName), clientInfo);
            if (!invoke.result.data || !invoke.result.data.uiapi) {
                return;
            }
            uiApi = invoke.result.data.uiapi;
            if (!(uiApi.query[tableName + ""] && uiApi.query[tableName + ""].edges[0])) return;
            pageInfo = uiApi.query[tableName + ""].pageInfo;
            afterData = pageInfo.endCursor;
        } else {
            clientInfo.after = afterData;
            invoke = invoker.invoke(getApiName(tableName) + " by after", clientInfo);
            if (!invoke.result.data || !invoke.result.data.uiapi) {
                return;
            }
            uiApi = invoke.result.data.uiapi;
            if (!(uiApi.query[tableName + ""] && uiApi.query[tableName + ""].edges[0])) return;
            pageInfo = uiApi.query[tableName + ""].pageInfo;
            afterData = pageInfo.endCursor;
        }
        let resultData = uiApi.query[tableName + ""].edges;
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
        isFirst = false;
        batchReadSender.send(result, tableName, {}, false);
        batchStart = Date.parse(new Date());
    } while (pageInfo.hasNextPage && isAlive());
}

function streamRead(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {
    if (!checkParam(tableNameList)) return;
    for (let index = 0; index < tableNameList.length; index++) {
        if (!isAlive()) break;
        let pageInfo = {"hasNextPage": true};
        let arr = [];
        let invoke;
        let first = true;
        do {
            if (!isAlive()) break;
            if (first) {
                invoke = invoker.invoke(getApiName(tableNameList[index]) + " stream read", clientInfo);
            } else {
                clientInfo.after = afterData;
                invoke = invoker.invoke(getApiName(tableNameList[index]) + " stream read by after", clientInfo);
            }
            if (!invoke.result.data || !invoke.result.data.uiapi) {
                break;
            }
            let resultMap = invoke.result.data.uiapi.query[tableNameList[index] + ""];
            let resultData = resultMap.edges;
            pageInfo = resultMap.pageInfo;
            afterData = pageInfo.endCursor;
            if (resultData[0] && resultData.length > 0) {
                for (let j = 0; j < resultData.length; j++) {
                    if (!isAlive()) break;
                    let resultItem = resultData[j];
                    let nod = resultItem.node;
                    if (!(Number(new Date(nod.LastModifiedDate.value)) > Number(new Date(batchStart)))) continue;
                    if (nod.LastModifiedDate.value === nod.CreatedDate.value) {
                        arr[j] = {
                            "eventType": "i",
                            "tableName": tableNameList[index],
                            "afterData": sendData(nod)
                        };
                    } else {
                        arr[j] = {
                            "eventType": "u",
                            "tableName": tableNameList[index],
                            "afterData": sendData(nod)
                        };
                    }
                }
                streamReadSender.send(arr, tableNameList[index], {});
                arr=[];
                first = false;
            }
            batchStart = Date.parse(new Date());
        } while(pageInfo.hasNextPage && isAlive())
    }
}

function commandCallback(connectionConfig, nodeConfig, commandInfo) {
    if (commandInfo.command === 'OAuth') {
        clientInfo.code = connectionConfig.code;
        let getToken = invoker.invokeWithoutIntercept("get access token", clientInfo);
        if (getToken.result) {
            connectionConfig.access_token = getToken.result.access_token;
            connectionConfig.refresh_token1 = getToken.result.refresh_token;
        }
        return connectionConfig;
    }
}

function updateToken(connectionConfig, nodeConfig, apiResponse) {
    if (apiResponse.httpCode === 503 || apiResponse.result.length > 0 && apiResponse.result[0].errorCode && apiResponse.result[0].errorCode === "REQUEST_LIMIT_EXCEEDED"){
        throw (apiResponse.result[0].message)
    }
    if (apiResponse.httpCode === 401 || (apiResponse.result && apiResponse.result.length > 0 && apiResponse.result[0].errorCode && apiResponse.result[0].errorCode === 'INVALID_SESSION_ID')) {
        clientInfo.refresh_token1 = connectionConfig.refresh_token1;
        try {
            let getToken = invoker.invokeWithoutIntercept("refresh token", clientInfo);
            if (getToken && getToken.result && getToken.result.access_token) {
                connectionConfig.access_token = getToken.result.access_token;
                return {"access_token": getToken.result.access_token};
            }
        } catch (e) {
            log.warn(e)
            throw(e);
        }
    }
    return null;
}

function connectionTest(connectionConfig) {
    clientInfo.Authorization = connectionConfig.access_token;
    let invoke = invoker.invoke('query opportunity', clientInfo);
    if (invoke.result.data) {
        return [
            {
                "test": " Check the Authorization.",
                "code": 1,
                "result": "Pass"
            },
            {
                "test": " Check the account read database permission.",
                "code": 1,
                "result": "Pass"
            }
        ];
    } else {
        return [{
            "test": " Check the account read database permission.",
            "code": -1,
            "result": "Not pass"
        }];
    }

}

