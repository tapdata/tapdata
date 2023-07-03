var invoker = loadAPI();
var doc_table_name = 'my_lark_doc';
var doc_info_page_size = 50;
function discoverSchema(connectionConfig) {
    return [{
        "name": doc_table_name,
        "fields": {
            "doc_token":{
                'type': 'String',
                'nullable': true,
                'isPrimaryKey': true,
                'primaryKeyPos': 1
            },
            "doc_type":{'type': 'String'},
            "title":{'type': 'String'},
            "owner_id":{'type': 'String'},
            "create_time":{'type': 'String'},
            "latest_modify_user":{'type': 'String',},
            "latest_modify_time":{'type': 'String'},
            "url":{'type': 'String'},
            "sec_label_name":{'type': 'String'},
            "content":{'type': 'String'}
        }
    }];
}

function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    try {
        let asFirst = !checkParam(offset) || (checkParam(offset) && !checkParam(offset.myLarkDoc));
        read(offset, tableName, (info, offsetState) => {
            let createTime = info.create_time;
            if (asFirst || createTime >= offsetState.myLarkDoc) {
                let content = getContent(info);
                if (null != content) {
                    batchReadSender.send({
                        "afterData": content,
                        "eventType": "i",
                        "tableName": doc_table_name,
                        "referenceTime": createTime,
                    }, doc_table_name, {'myLarkDoc': createTime});
                }
            }
        })
    }catch (e){
        log.error("Error to batch read table {}, An exception occurred: {} ", tableName, 'undefined' !== e.message && null != e.message ? e.message : e)
    }
}

var stream_cache_doc = {};
var stream_cache_time = 0;
function streamRead(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {
    for (let index = 0; index < tableNameList.length; index++) {
        let tableName = tableNameList[index];
        try {
            read(offset, tableName, (info, offsetState) => {
                let createTime = info.create_time;
                let updateTime = info.latest_modify_time;
                let isCreate = (checkParam(updateTime) && updateTime >= offsetState.myLarkDoc) ?
                    0 : ((checkParam( updateTime) && createTime >= offsetState.myLarkDoc) ? 1 : -1);
                if (isCreate >= 0) {
                    let isCache = isCreate === 1 ? stream_cache_time === createTime : stream_cache_time === updateTime;
                    let tempId = isCreate === 1 ? createTime : updateTime;
                    //如果已经缓存过当前秒的记录，则判断这条数据是否被消费（最新一秒内的被消费的数据Id就会缓存）
                    if (!isCache || (isCache && !checkParam(stream_cache_doc[ tempId + "" + info.doc_token]))) {
                        let content = getContent(info);
                        if (null != content) {
                            streamReadSender.send({
                                "afterData": content,
                                "eventType": isCreate === 1 ? "i" : "u",
                                "tableName": doc_table_name,
                                "referenceTime": isCreate === 1 ? createTime : updateTime,
                            }, doc_table_name, {'myLarkDoc': createTime});
                            if (!isCreate) {
                                stream_cache_doc = {};
                                stream_cache_time = tempId;
                            }
                            stream_cache_doc[tempId + "" + info.doc_token] = true;
                        }
                    }
                }
            })
        }catch (e){
            log.error("Error to stream read table {}, An exception occurred: {} ", tableName, 'undefined' !== e.message && null != e.message ? e.message : e)
        }
    }
}

function connectionTest(connectionConfig) {
    let app = null;
    try {
        app = invoker.invoke("AppInfo").result;
        let isApp = 1;
        if (!checkParam(app) && !checkParam(app.data) && !checkParam(app.data.app)){
            log.warn("Cannot get Application info, and application name will be empty now.")
            isApp = -1;
        }
        let result = [{
            "test": "Get App info", "code": isApp,
            "result": isApp === 1 ? "App name is:" + app.data.app.app_name : "Can not get App info, please check you App ID and App Secret."
        }];
        if (isApp === 1){
            result.push({
                "test": "Read log",
                "code": 1,
                "result": "Pass"
            });
        }
        return result;
    }catch (e){
        return [{"test":" Input parameter check ", "code": -1,
            "result": "Can not get App info, please check you App ID and App Secret."
        }];
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
    //@TODO GetAppToken
    let result = invoker.invokeWithoutIntercept("GetAppToken");
    if (checkParam(apiResponse.result.code) && result.result.code === 0) {
        return {"access_token": result.result.tenant_access_token};
    } else {
        log.warn('Cannot get tenant access token, please check your app_id or app_secret or check api named GetAppToken. ');
        return null;
    }
}

function timestampToStreamOffset(time){
    return ('undefined' === time || null == time) ? {'myLarkDoc' : parseInt(new Date().getTime() / 1000 )} : time;
}

//检查参数
function checkParam(param) {
    return 'undefined' !== param && null != param;
}

//获取知识空间列表
function getDocInfo(offset, handle){
    let hasMore = true;
    let pageToken = null;
    while(isAlive() && hasMore) {
        /**
         * {
                "data": {
                    "items": [
                        {
                            "name": "知识空间",
                            "description": "知识空间描述",
                            "space_id": "1565676577122621"
                        }
                    ],
                    "page_token": "1565676577122621",
                    "has_more": true
                }
            }
         * */
            //@TODO SpaceList
        let myWiKi = invoker.invoke("SpaceList",isParam(pageToken) ? {"page_token": pageToken} : {});
        let wikiData = myWiKi.result.data;
        if (!checkParam(wikiData)){
            log.warn("Fail to obtain a list of knowledge spaces.");
            return;
        }

        let wikiDataItems = wikiData.items;
        if (!checkParam(wikiDataItems)){
            log.warn("Fail to obtain a list of knowledge spaces, space list is empty.");
            return ;
        }

        for (let index = 0; index < wikiDataItems.length; index++){
            let wikiSpaceId = wikiDataItems[index].space_id;
            if (!checkParam(wikiSpaceId)){
                log.warn("Fail to obtain a list of knowledge spaces, space id is empty and this space will be ignore.");
                continue;
            }
            getChildFromSpace(wikiSpaceId, '', offset, handle);
        }
        hasMore = checkParam(wikiData.has_more) ? wikiData.has_more : false;
        pageToken = wikiData.page_token;
    }
    if (docList.length > 0){
        getAllDocInfo(docList, offset, handle);
        docList = []
    }
}

let docList = [];
//获取子空间
function getChildFromSpace(spaceId, parentNodeToken, offset, handle){
    let pageTokenOfChild = null;
    let hasMore = true;
    while (isAlive() && hasMore) {
        let dataConfig = {
            "space_id": spaceId,
            "page_size": doc_info_page_size,
            "page_token": checkParam(pageTokenOfChild) ? pageTokenOfChild : "",
            "parent_node_token": checkParam(parentNodeToken) ? parentNodeToken : ""
        }
        /**
         * {
          "data": {
            "has_more": false,
            "items": [
              {
                "creator": "ou_056b8040b36ba4ee19473e08cf56d673",
                "has_child": false,
                "node_create_time": "1682390369",
                "node_token": "wikcnyAasqtrKe69SiprZS1vgZu",
                "node_type": "origin",
                "obj_create_time": "1682390368",
                "obj_edit_time": "1682390382",
                "obj_token": "DhlRdLjOLojjRGxOyT1cm6LNnsb",
                "obj_type": "docx",
                "origin_node_token": "wikcnyAasqtrKe69SiprZS1vgZu",
                "origin_space_id": "7225808628723400706",
                "owner": "ou_056b8040b36ba4ee19473e08cf56d673",
                "parent_node_token": "wikcnI7ZnBMdJiIu3npziEH5iOe",
                "space_id": "7225808628723400706",
                "title": "Test"
              }
            ],
            "page_token": ""
        }
         * */
            //@TODO SubNodes
        let child = invoker.invoke("SubNodes", dataConfig);
        let childData = child.result.data;
        if (!checkParam(childData)) {
            log.warn("Fail to obtain a list of sub nodes in the knowledge space, request body: {}.", JSON.stringify(dataConfig));
            return;
        }
        let childDataItem = childData.items;
        if (!checkParam(childDataItem)) {
            log.warn("Fail to obtain a list of sub nodes in the knowledge space, list is empty, request body: {}.", JSON.stringify(dataConfig));
            return;
        }
        for (let index = 0; index < childDataItem.length; index++) {
            let item = childDataItem[index];

            let fileType = item.obj_type;
            if (checkParam(fileType) && (
                "doc" === fileType || "docx" === fileType
            )){
                let fileToken = item.obj_token;
                if (checkParam(fileToken)){
                    docList.push({"doc_token": fileToken, "doc_type": fileType});
                    if (docList.length === doc_info_page_size){
                        getAllDocInfo(docList, offset, handle);
                        docList = []
                    }
                }
            }else {
                //文件类型不支持
                let fileName = checkParam(item.Title) ? ", name is" + item.Title : "";
                log.info("File type is {} {}, and will be ignore this file, only doc and docx can be read.", fileType, fileName);
            }

            let hasChild = item.has_child;
            if (checkParam(hasChild) && hasChild) {
                let nodeTokenId = item.node_token;
                if (checkParam(nodeTokenId)) {
                    doneWhenHasChild(spaceId, nodeTokenId, offset, handle);
                }
            }
        }
        hasMore = checkParam(childData.has_more) ? childData.has_more : false;
        pageTokenOfChild = childData.page_token;
    }
}

//存在子空间再获取子空间
function doneWhenHasChild(spaceId, childSpaceId, offset, handle){
    getChildFromSpace(spaceId, childSpaceId, offset, handle);
}

//获取文档元数据列表，包括创建时间、更新时间
function getAllDocInfo(infoConfig, offset, handle){
    //@TODO Get Doc Info
    let docInfo = invoker.invoke("Get Doc Info", {"infoConfig":JSON.stringify(infoConfig)});
    let infoData = docInfo.result.data;
    if (!checkParam(infoData)){
        log.info("Can not get mates for files by api: https://open.feishu.cn/open-apis/drive/v1/metas/batch_query, file's config is {}", JSON.stringify(infoConfig))
        return;
    }
    let infoDataMetas = infoData.metas;
    if (!checkParam(infoDataMetas)){
        log.info("Can not get mates for files, file's config is {}", JSON.stringify(infoConfig))
        return;
    }
    for (let index = 0; index < infoDataMetas.length; index++) {
        let infoItem = infoDataMetas[index];
        handle(infoItem, offset);
    }
}

//获取村文本内容
function getContent(infoItem){
    if (!checkParam(infoItem)){
        log.info("One file not have mate info, will be ignore this file.")
        return null;
    }
    let docItemToken = infoItem.doc_token;
    if (!checkParam(docItemToken)){
        log.info("One file not have file token in mate info, will be ignore this file.")
        return null;
    }
    let docItemType = infoItem.doc_type;
    if (!checkParam(docItemType)) {
        log.info("One file not have file type in mate info, will be ignore this file.")
        return null;
    }
    //获取文档文本内容
    //@TODO docToken
    let docx = invoker.invoke(docItemType,{"docToken" : docItemToken});
    let docData = docx.result.data;
    if (!checkParam(docData)){
        log.warn("Cant get file content data, file token is {}, type is {}.", docItemToken, docItemType);
        return null;
    }
    let docDataContent = docData.content;
    if (!checkParam(docDataContent)){
        log.warn("Cant get file content, file token is {}, type is {}.", docItemToken, docItemType);
        return null;
    }
    infoItem.content = docDataContent;
    return infoItem;
}

function read(offset, tableName, handle){
    if(tableName !== doc_table_name) return;
    if (!checkParam(offset) || Object.keys(offset).length <= 0 ){
        offset = {
            "myLarkDoc" : parseInt(new Date().getTime() / 1000)
        }
    }
    getDocInfo(offset, handle);
}