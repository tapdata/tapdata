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
function discover_schema(connectionConfig) {
    return ['Leads','Contacts','Accounts','Potentials','Quotes'];
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
function batch_read(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    log.error("begin read");
    if(!offset){
        offset = {
            page:1,
            tableName:tableName
        };
    }
    iterateAllData('getData', offset, (result, offsetNext, error) => {
    log.error("read result"+result);
        if(result && result !== ''){
            let haveNext = false;
            if(result.info && result.info.more_records && result.info.page){
                if(!offsetNext.page){
                    offsetNext.page = 1;
                }
                offsetNext.page = offsetNext.page + 1;
                haveNext = true;
            }
            batchReadSender.send(result.data,tableName);
            if(!haveNext){
                return false
            }
        }
        return isAlive();
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
var batchStart = nowDate();
function stream_read(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {
    log.error("------- begin stream read ---------");
    if (!isParam(offset) || null == offset || typeof(offset) != 'object') offset = {tableName:tableNameList[0],page: 1,Conditions:[{Key: 'UPDATED_AT',Value: batchStart + '_' + nowDate()}]} ;
    let condition = firstElement(offset.Conditions);
    offset.Conditions = [{Key:"UPDATED_AT",Value: isParam(condition) && null != condition ? firstElement(condition.Value.split('_')) + '_' + nowDate(): batchStart + '_' + nowDate()}];
    offset['If-Modified-Since'] = (new Date( new Date().getTime() - 60000)).toISOString();
    log.error("------- offset ---------" + offset);
    iterateAllData('getDataA', offset, (result, offsetNext, error) => {
        log.error("read result"+result);
        if(result && result !== ''){
            let haveNext = false;
            if(result.info && result.info.more_records && result.info.page){
                if(!offsetNext.page){
                    offsetNext.page = 1;
                }
                offsetNext.page = offsetNext.page + 1;
                haveNext = true;
            }
            streamReadSender.send(result.data,nodeConfig.tableName,offsetNext,true);
            if(!haveNext){
                return false
            }
        }
        return isAlive();
    });

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
function connection_test(connectionConfig) {
    return [{
        "TEST": "Example test item",
        "CODE": 1,
        "RESULT": "Pass"
    }];
}


/**
 *
 * @param connectionConfig
 * @param nodeConfig
 * @param commandInfo
 * */
function command_callback(connectionConfig, nodeConfig, commandInfo) {
    if (commandInfo.command === 'TokenCommand') {
        log.error("--------begin read:"+connectionConfig);
        let body = {
            client_id:connectionConfig.client_id,
            client_secret:connectionConfig.client_secret,
            code:connectionConfig.code
        };
        //let refreshToken = rest.post('https://accounts.zoho.com.cn/oauth/v2/token', body,{'Content-Type':'application/x-www-form-urlencoded'});
        log.error("--------body-----:"+body);
        let refreshToken = invoker.invoke("getToken",body,"POST");
        //log.error('-------result:'+tapUtil.fromJson(refreshToken));
        if (refreshToken && refreshToken.result && refreshToken.result.access_token) {
            return {
                'setValue':{
                    accessToken:{data:refreshToken.result.access_token},
                    refreshToken:{data:refreshToken.result.refresh_token},
                    getTokenMsg:{data:'123'}
                }
            };
        }else{
            return refreshToken;
        }
    }
}

/**
 * @param connectionConfig
 * @param nodeConfig
 * @param tableNameList
 * @param eventDataMap  eventDataMap.data is sent from WebHook
 *
 * @return array with data maps.
 *  Each data includes five parts:
 *      - EVENT_TYPE : event type ,only with : i , u, d. Respectively insert, update, delete;
 *      - TABLE_NAME : Data related table;
 *      - REFERENCE_TIME : Time stamp of event occurrence;
 *      - AFTER_DATA : After the event, only AFTER_DATA can be added or deleted as a result of the data
 *      - BEFORE_DATA : Before the event, the result of the data will be BEFORE_DATA only if the event is modified
 *  please return with: [
 *      {
 *          "EVENT_TYPE": "i/u/d",
 *          "TABLE_NAME": "${example_table_name}",
 *          "REFERENCE_TIME": Number(),
 *          "AFTER_DATA": {},
 *          "BEFORE_DATA":{}
 *      },
 *      ...
 *     ]
 * */
function web_hook_event(connectionConfig, nodeConfig, tableNameList, eventDataMap) {

    //return [
    //     {
    //         "EVENT_TYPE": "i/u/d",
    //         "TABLE_NAME": "${example_table_name}",
    //         "REFERENCE_TIME": Number(),
    //         "AFTER_DATA": {},
    //         "BEFORE_DATA":{}
    //     }
    //]
}

/**
 * [
 *  {
 *      "EVENT_TYPE": "i/u/d",
 *      "TABLE_NAME": "${example_table_name}",
 *      "REFERENCE_TIME": Number(),
 *      "AFTER_DATA": {},
 *      "BEFORE_DATA":{}
 *  },
 *  ...
 * ]
 * @param connectionConfig
 * @param nodeConfig
 * @param eventDataList type is js array with data maps.
 *  Each data includes five parts:
 *      - EVENT_TYPE : event type ,only with : i , u, d. Respectively insert, update, delete;
 *      - TABLE_NAME : Data related table;
 *      - REFERENCE_TIME : Time stamp of event occurrence;
 *      - AFTER_DATA : After the event, only AFTER_DATA can be added or deleted as a result of the data
 *      - BEFORE_DATA : Before the event, the result of the data will be BEFORE_DATA only if the event is modified
 *  @return true or false, default true
 * */
function write_record(connectionConfig, nodeConfig, eventDataList) {

    //return true;
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
function update_token(connectionConfig, nodeConfig, apiResponse) {
    log.error('+++++ begin refreshToken +++++' + apiResponse.code);
    if (apiResponse.httpCode === 401 || (apiResponse.result && apiResponse.result.code === 'INVALID_TOKEN')) {
        //connectionConfig.refresh_token = connectionConfig.refreshToken;
        //log.error('+++++ begin refreshToken +++++ refreshToken :' + connectionConfig.refreshToken);
        try{
            let refreshToken = invoker.invokeWithoutIntercept("refreshToken");
            log.error('+++++ refreshToken +++++',refreshToken);
            if(refreshToken && refreshToken.result &&refreshToken.result.access_token){
                log.error('+++++ refreshToken access_token +++++',refreshToken.result.access_token);
                return {"accessToken": refreshToken.result.access_token};
            }
        }catch (e) {
            log.error(' -------- error -------',e)
        }
    }
    return null;
}