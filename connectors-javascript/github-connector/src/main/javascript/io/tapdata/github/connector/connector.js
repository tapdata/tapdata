/**
 * @return The returned result cannot be empty and must conform to one of the following forms:
 *      (1)Form one:  A string (representing only one table name)
 *          return 'example_table_name';
 *      (2)Form two: A String Array (It means that multiple tables are returned without description, only table name is provided)
 *          return ['example_table_1', 'example_table_2', ... , 'example_table_n'];
 *      (3)Form three: A Object Array ( The complex description of the table can be used to customize the properties of the table )
 *          return [
 *           {
 *               "name: '${example_table_1}',
 *               "fields": {
 *                   "${example_field_name_1}":{
 *                       "type":"${field_type}",
 *                       "default:"${default_value}"
 *                   },
 *                   "${example_field_name_2}":{
 *                       "type":"${field_type}",
 *                       "default:"${default_value}"
 *                   }
 *               }
 *           },
 *           '${example_table_2}',
 *           ['${example_table_3}', '${example_table_4}', ...]
 *          ];
 * @param connectionConfig  Configuration property information of the connection page
 * */
function discoverSchema(connectionConfig) {
    return [{
        "name":'Issues and Pull requests',
        "fields":{
            'id':{
                'type':'Number',
                'comment':'',
                'nullable':false,
                'isPrimaryKey':true,
                'primaryKeyPos':1
            },
            'node_id':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'url':{
                'type':'String',
                'comment':'URL for the issue',
                'nullable':true
            },
            'repository_url':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'labels_url':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'comments_url':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'events_url':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'html_url':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'number':{
                'type':'Number',
                'comment':'Number uniquely identifying the issue within its repository',
                'nullable':true
            },
            'state':{
                'type':'String',
                'comment':'State of the issue; either \'open\' or \'closed\'',
                'nullable':false
            },
            'state_reason':{
                'type':'String',
                'comment':'The reason for the current state',
                'nullable':true
            },
            'title':{
                'type':'String',
                'comment':'Title of the issue',
                'nullable':false
            },
            'body':{
                'type':'String',
                'comment':'Contents of the issue',
                'nullable':true
            },
            'user':{
                'type':'Object',
                'comment':'',
                'nullable':false
            },
            'labels':{
                'type':'Array',
                'comment':'Labels to associate with this issue; pass one or more label names to replace the set of labels on this issue; send an empty array to clear all labels from the issue; note that the labels are silently dropped for users without push access to the repository',
                'nullable':false
            },
            'assignee':{
                'type':'Object',
                'comment':'',
                'nullable':false
            },
            'milestone':{
                'type':'Object',
                'comment':'',
                'nullable':false
            },
            'locked':{
                'type':'Boolean',
                'comment':'',
                'nullable':false
            },
            'active_lock_reason':{
                'type':'String',
                'comment':'',
                'nullable':true
            },
            'comments':{
                'type':'Number',
                'comment':'',
                'nullable':false
            },
            'pull_request':{
                'type':'Object',
                'comment':'',
                'nullable':true
            },
            'closed_at':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'created_at':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'updated_at':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'draft':{
                'type':'Boolean',
                'comment':'',
                'nullable':true
            },
            'closed_by':{
                'type':'Object',
                'comment':'',
                'nullable':true
            },
            'body_html':{
                'type':'String',
                'comment':'',
                'nullable':true
            },
            'body_text':{
                'type':'String',
                'comment':'',
                'nullable':true
            },
            'timeline_url':{
                'type':'String',
                'comment':'',
                'nullable':false
            },
            'repository':{
                'type':'String',
                'comment':'A repository on GitHub.',
                'nullable':true
            },
            'performed_via_github_app':{
                'type':'Object',
                'comment':'',
                'nullable':true
            },
            'author_association':{
                'type':'String',
                'comment':'How the author is associated with the repository.',
                'nullable':false
            },
            'reactions':{
                'type':'Object',
                'comment':'',
                'nullable':true
            }
        }
    }];
}

/**
 *
 * @param connectionConfig  Configuration property information of the connection page
 * @param nodeConfig  Configuration attribute information of node page
 * @param offset Breakpoint information, which can save paging condition information
 * @param tableName  The name of the table to obtain the full amount of phase data
 * @param pageSize Processing number of each batch of data in the full stage
 * @param batchReadSender  Sender of submitted data
 * */
const tableMapping = {
    'Issues and Pull requests':'issues'
}
function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    if(!offset || !offset.tableName){
        offset = {
            page:1,
            tableName:tableName,
            since: new Date('1990-01-01').toISOString()
        };
    }
    iterateAllData(tableMapping[tableName], offset, (result, offsetNext, error) => {
        let haveNext = false;
        if(result && result !== ''){
            if(result && result.length > 0){
                if(!offsetNext.page){
                    offsetNext.page = 1;
                }
                offsetNext.page = offsetNext.page + 1;
                haveNext = true;
            }
            batchReadSender.send(result,tableName,offset);
            if(!haveNext){
                return false
            }
        }else{
          return false
        }
        return isAlive() && haveNext;
    });
}


/**
 *
 * @param connectionConfig  Configuration property information of the connection page
 * @param nodeConfig
 * @param offset
 * @param tableNameList
 * @param pageSize
 * @param streamReadSender
 * */

var batchStart = dateUtils.nowDate();
var startTime = new Date();
function streamRead(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {
    if (!isParam(offset) || null == offset || typeof(offset) != 'object') offset = {};
    for(let x in tableNameList) {
      let tableName = tableNameList[x];
      let isFirst = false;
      if(!offset[tableName]){
        offset[tableName] = {tableName:tableName, page: 1, Conditions:[{Key: 'UPDATED_AT',Value: batchStart + '_' + dateUtils.nowDate()}]} ;
        isFirst = true;
      }
        offset[tableName].page = 1;
        let condition = arrayUtils.firstElement(offset[tableName].Conditions);
        offset[tableName].Conditions = [{Key:"UPDATED_AT",Value: isParam(condition) && null != condition ? arrayUtils.firstElement(condition.Value.split('_')) + '_' + dateUtils.nowDate(): batchStart + '_' + dateUtils.nowDate()}];
        if(isFirst){
        offset[tableName]['since'] = new Date(startTime.getTime() - 65000).toISOString();
        }
        iterateAllData('issues', offset[tableName], (result, offsetNext, error) => {
            let haveNext = false;
            let arr = [];
            if(result && result !== ''){
                if(result && result.length > 0){
                    if(!offsetNext.page){
                        offsetNext.page = 1;
                    }
                    offsetNext.page = offsetNext.page + 1;
                    haveNext = true;
                }else{
                    offsetNext.page = 1;
                }
                for(let x in result){
                    if( result[x].updated_at && offset[tableName]['since'] < result[x].updated_at){
                        offset[tableName]['since'] = new Date(new Date(result[x].updated_at).getTime() + 1000).toISOString();
                    }
                    let item = {
                        "eventType": "i",
                        "tableName": tableName,
                        "afterData": result[x],
                        "referenceTime":  Number(new Date())
                    };
                    if (result[x].created_at === result[x].updated_at) {
                        item.eventType = 'i';
                    } else {
                        item.eventType = 'u';
                    }
                    arr.push(item);
                }
                streamReadSender.send(arr,tableName,offset);
                if(!haveNext){
                    return false
                }
            } else{
                offsetNext.page = 1;
                return false
            }
            return isAlive() && haveNext;
        });
    }
    streamReadSender.send(offset);
}

let clientInfo = {
    "client_id": "3befd8286de3bc48e082",// "Iv1.cb38266944261353",
    "client_secret": "d522f8f32f41147f64ef5b3d7ceffe1a3750cf44"//"a04776e494712e0a73e80b80367bd37ebbc7626b"
}
/**
 * @return The returned result is not empty and must be in the following form:
 *          [
 *              {"TEST": String, "CODE": Number, "RESULT": String},
 *              {"TEST": String, "CODE": Number, "RESULT": String},
 *              ...
 *          ]
 *          param - TEST :  The type is a String, representing the description text of the test item.
 *          param - CODE :  The type is a Number, is the type of test result. It can only be [-1, 0, 1], -1 means failure, 1 means success, 0 means warning.
 *          param - RESULT : The type is a String, descriptive text indicating test results.
 * @param connectionConfig  Configuration property information of the connection page
 * */
function connectionTest(connectionConfig) {
    try {
        let res = [];
        let userRepos = invoker.invoke("getUserRepos", {});
        if (userRepos && userRepos.result) {
            res.push({
                "test": "Test Authorization",
                "code": 1,
                "result": "Pass"
            });
            res.push({
                "test": "Get User Repos",
                "code": 1,
                "result": "Pass"
            });
            res.push({
                "test": "Read log",
                "code": 1,
                "result": "Pass"
            });
        } else {
            res.push({
                "test": "Get User Repos",
                "code": 1,
                "result": "Error"
            })
        }
        return res;
    } catch (e) {
        return [{
            "test": "Test Connection",
            "code": -1,
            "result": e
        }]
        log.warn(e)
    }
}


/**
 *
 * @param connectionConfig
 * @param nodeConfig
 * @param commandInfo  scope=repo user&
 * */
function commandCallback(connectionConfig, nodeConfig, commandInfo) {
    if (commandInfo.command === 'OAuth'){
        let obj = {code:connectionConfig.code};
        Object.assign(obj,clientInfo)
        let getToken = invoker.invokeWithoutIntercept("getToken",obj);
        if(getToken.result){
            connectionConfig.access_token = getToken.result.access_token;
        }
        return connectionConfig;
    }
    if (commandInfo.command === 'getUserRepos') {
        let rs = [];
        let page = 1;
        if(commandInfo.args && commandInfo.args.page && commandInfo.args.page > 1){
            return {
                "items": [],
                "page": commandInfo.args.page,
                "size": 100,
                "total": 0
            };
        }
        while(true){
            let res = invoker.invoke("getUserRepos",{page:page,access_token:connectionConfig.access_token});
            if(res && res.result && res.result.length > 0){
                for(let x in res.result){
                    if(commandInfo.args && commandInfo.args.key && commandInfo.args.key !== ''){
                        if(res.result[x].full_name.includes(commandInfo.args.key)){
                            rs.push(res.result[x])
                        }
                    }else{
                        rs.push(res.result[x])
                    }
                }
                page++;
            }else{
                break;
            }
        }
        return {
            "items": arrayUtils.convertList(rs, {'full_name':'label', 'full_name':'value'}),
            "page": 1,
            "size": rs.length,
            "total": rs.length
        };
    }
}


/**
 * This method is used to update the access key
 *  @param connectionConfig :type is Object
 *  @param nodeConfig :type is Object
 *  @param apiResponse :The following valid data can be obtained from apiResponse : {
 *              result :  type is Object, Return result of interface call
 *              httpCode : type is Integer, Return http code of interface call
 *          }
 *
 *  @return must be {} or null or {"key":"value",...}
 *      - {} :  Type is Object, but not any key-value, indicates that the access key does not need to be updated, and each interface call directly uses the call result
 *      - null : Semantics are the same as {}
 *      - {"key":"value",...} : Type is Object and has key-value ,  At this point, these values will be used to call the interface again after the results are returned.
 * */
function updateToken(connectionConfig, nodeConfig, apiResponse) {
    return;
    if (apiResponse.httpCode === 401 || (apiResponse.result && apiResponse.result.message === 'Requires authentication')) {
        try{
            let getToken = invoker.invokeWithoutIntercept("refreshToken",clientInfo);
            if(getToken && getToken.result && getToken.result.access_token){
                return {"access_token":  getToken.result.access_token};
            }
        }catch (e) {
            log.warn(e)
            throw(e);
        }
    }
    return null;
}