var invoker = loadAPI();
var doc_table_name = 'my_lark_doc';
function discoverSchema(connectionConfig) {
    return [{
        "name": doc_table_name,
        "fields": {
            "doc_token":{
                'type': 'String',
                'comment': '',
                'nullable': true,
                'isPrimaryKey': true,
                'primaryKeyPos': 1
            },
            "doc_type":{
                'type': 'String',
                'comment': '',
                'nullable': false,
                'isPrimaryKey': false
            },
            "title":{
                'type': 'String',
                'comment': '',
                'nullable': false,
                'isPrimaryKey': false
            },
            "owner_id":{
                'type': 'String',
                'comment': '',
                'nullable': false,
                'isPrimaryKey': false
            },
            "create_time":{
                'type': 'String',
                'comment': '',
                'nullable': false,
                'isPrimaryKey': false
            },
            "latest_modify_user":{
                'type': 'String',
                'comment': '',
                'nullable': false,
                'isPrimaryKey': false
            },
            "latest_modify_time":{
                'type': 'String',
                'comment': '',
                'nullable': false,
                'isPrimaryKey': false
            },
            "url":{
                'type': 'String',
                'comment': '',
                'nullable': false,
                'isPrimaryKey': false
            },
            "sec_label_name":{
                'type': 'String',
                'comment': '',
                'nullable': false,
                'isPrimaryKey': false
            },
            "content":{
                'type': 'String',
                'comment': '',
                'nullable': false,
                'isPrimaryKey': false
            }
        }
    }];
}

function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    try {
        read(offset, tableName, (info, offsetState) => {
            let createTime = info.create_time;
            if (createTime >= offsetState.myLarkDoc) {
                batchReadSender.send({
                    "afterData": info,
                    "eventType": "i" ,
                    "tableName": doc_table_name,
                    "referenceTime": createTime,
                }, doc_table_name, {'myLarkDoc': createTime});
                hasHandelCache0 += ("token_" + info.doc_token+ ",");
            }
        })
    }catch (e){
        log.error("Error to batch read table {}, An exception occurred: {} ", tableName, 'undefined' !== e.message && null != e.message ? e.message : e)
    }
}

function read(offset, tableName, handle){
    if(tableName !== doc_table_name) return;
    if (checkParam(offset) || Object.keys(offset).length <= 0 ){
        offset = {
            "myLarkDoc" : new Date()
        }
    }
    let hasNext = true;
    //获取清单列表
    do {
        let docList = invoker.invoke("Get File List");
        let data = docList.result.data;
        if (checkParam(data)){
            log.warn("Can not get file list with api: https://open.feishu.cn/open-apis/drive/v1/files, msg: {}", docList.result.msg);
            break;
        }
        let files = data.files;
        if (checkParam(files)){
            log.warn("Not find any file.")
            break;
        }

        let infoConfig = [];
        for (let index = 0; index < files.length; index++) {
            let fileTemp = files[index];
            if (checkParam(fileTemp)){
                log.info("File info is empty.");
                continue;
            }
            let fileType = fileTemp.type;
            if (checkParam(fileType)){
                log.info("Unknown file type, will be ignore this file.");
                continue;
            }
            let fileToken = fileTemp.token;
            if (checkParam(fileToken)){
                log.info("Unknown file token, will be ignore this file.");
                continue;
            }
            switch (fileType) {
                case 'doc':
                case 'docx':
                    infoConfig.push({"doc_token": fileToken, "doc_type": fileType});
                    break;
                default: log.info("File type is {}, and will be ignore this file, only doc and docx can be read.", fileType);
            }
        }


        //获取文件元数据
        let docInfo = invoker.invoke("Get Doc Info", {"infoConfig":JSON.stringify(infoConfig)});
        let infoData = docInfo.result.data;
        if (checkParam(infoData)){
            log.info("Can not get mates for files by api: https://open.feishu.cn/open-apis/drive/v1/metas/batch_query, file's config is {}", JSON.stringify(infoConfig))
            continue;
        }
        let infoDataMetas = infoData.metas;
        if (checkParam(infoDataMetas)){
            log.info("Can not get mates for files, file's config is {}", JSON.stringify(infoConfig))
            continue;
        }
        for (let index = 0; index < infoDataMetas.length; index++) {
            let infoItem = infoDataMetas[index];
            if (checkParam(infoItem)){
                log.info("One file not have mate info, will be ignore this file.")
                continue;
            }
            let docItemToken = infoItem.doc_token;
            if (checkParam(docItemToken)){
                log.info("One file not have file token in mate info, will be ignore this file.")
                continue;
            }
            let docItemType = infoItem.doc_type;
            if (checkParam(docItemType)) {
                log.info("One file not have file type in mate info, will be ignore this file.")
                continue;
            }

            //获取文档文本内容
            let docx = invoker.invoke(docItemType,{"docToken" : docItemToken});
            let docData = docx.result.data;
            if (checkParam(docData)){
                log.warn("Cant get file content data, file token is {}, type is {}.", docItemToken, docItemType);
                continue;
            }
            let docDataContent = docData.content;
            if (checkParam(docDataContent)){
                log.warn("Cant get file content, file token is {}, type is {}.", docItemToken, docItemType);
                continue;
            }
            infoItem.content = docDataContent;
            handle(infoItem, offset);
        }
        hasNext = (checkParam(data.has_more)) ? false : data.has_more;
    }while (isAlive() && hasNext);
}

function streamRead(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {
    for (let index = 0; index < tableNameList.length; index++) {
        let tableName = tableNameList[index];
        try {
            read(offset, tableName, (info, offsetState) => {
                let createTime = info.create_time;
                let updateTime = info.latest_modify_time;
                let isCreate = ('undefined' !== updateTime && null != updateTime && updateTime >= offsetState.myLarkDoc) ?
                    0 : ((checkParam( updateTime) && createTime >= offsetState.myLarkDoc) ? 1 : -1);
                if (isCreate >= 0) {
                    streamReadSender.send({
                        "afterData": info,
                        "eventType": isCreate === 1 ? "i" : "u",
                        "tableName": doc_table_name,
                        "referenceTime": isCreate === 1 ? createTime : updateTime,
                    }, doc_table_name, {'myLarkDoc': createTime});
                }
            })
        }catch (e){
            log.error("Error to stream read table {}, An exception occurred: {} ", tableName, 'undefined' !== e.message && null != e.message ? e.message : e)
        }
    }
}

function connectionTest(connectionConfig) {
    let msg = "";
    let status = 1;
    try {
        let userInfo = invoker.invoke("Get User Info");
        let name = userInfo.result.data.name;
        msg = "Hello! " + name;
    }catch (e){
        msg = "Error, connection test fail."
        status = -1;
    }
    return [{
        "test": "Check your authorize",
        "code": status,
        "result": msg
    }];
}

function commandCallback(connectionConfig, nodeConfig, commandInfo) {
    if (commandInfo.command === 'OAuth') {
        let getToken = invoker.invokeWithoutIntercept("Get AccessToken", {
            "code": commandInfo.connectionConfig.code
        });
        if (getToken.result) {
            connectionConfig.refresh_token = getToken.result.data.refresh_token;
            connectionConfig.access_token = getToken.result.data.access_token;
        }
        return connectionConfig;
    }
}

function updateToken(connectionConfig, nodeConfig, apiResponse) {
    if (!checkParam(apiResponse.result.code) ||
        (apiResponse.result.code !== 99991663 && apiResponse.result.code !== 99991661)) {
        if (!checkParam(apiResponse.result.code) && apiResponse.httpCode !== 0){
            apiError.check(apiResponse.result.code);
        }
        return null;
    }
    let result = invoker.invokeWithoutIntercept("Refresh AccessToken");
    if (checkParam(apiResponse.result.code) && result.result.code === 0) {
        return {
            "access_token": result.result.data.access_token,
            "refresh_token": result.result.data.refresh_token
        };
    } else {
        log.warn('Cannot get tenant access token, please check your app_id or app_secret or check api named GetAppToken. ');
        return null;
    }
}

function timestampToStreamOffset(time){
    return ('undefined' === time || null == time) ? {'myLarkDoc' : new Date()} : time;
}

function checkParam(param) {
    return 'undefined' !== param && null != param;
}

