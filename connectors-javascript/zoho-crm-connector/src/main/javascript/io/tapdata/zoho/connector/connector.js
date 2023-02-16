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
function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    if(!offset){
        offset = {
            page:1,
            tableName:tableName
        };
    }
    iterateAllData('getData', offset, (result, offsetNext, error) => {
        let haveNext = false;
        if(result && result !== ''){
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
        let condition = firstElement(offset[tableName].Conditions);
        offset[tableName].Conditions = [{Key:"UPDATED_AT",Value: isParam(condition) && null != condition ? firstElement(condition.Value.split('_')) + '_' + nowDate(): batchStart + '_' + nowDate()}];
        if(isFirst){
        offset[tableName]['If-Modified-Since'] = DateUtil.timeStamp2Date((startTime.getTime() - 60000)+"", "yyyy-MM-dd'T'HH:mm:ssXXX");
        } else {
        offset[tableName]['If-Modified-Since'] = DateUtil.timeStamp2Date((new Date().getTime() - 60000)+"", "yyyy-MM-dd'T'HH:mm:ssXXX");
        }
        iterateAllData('getDataA', offset[tableName], (result, offsetNext, error) => {
            let haveNext = false;
            if(result && result !== ''){
                if(result.info && result.info.more_records && result.info.page){
                    if(!offsetNext.page){
                        offsetNext.page = 1;
                    }
                    offsetNext.page = offsetNext.page + 1;
                    haveNext = true;
                }
                streamReadSender.send(result.data,tableName);
                if(!haveNext){
                    return false
                }
            }
            return isAlive() && haveNext;
        });
    }
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
function commandCallback(connectionConfig, nodeConfig, commandInfo) {
    if (commandInfo.command === 'TokenCommand') {
        let body = {
            client_id:connectionConfig.client_id,
            client_secret:connectionConfig.client_secret,
            code:connectionConfig.code
        };
        let refreshToken = invoker.invoke("getToken",body,"POST");
        if (refreshToken && refreshToken.result && refreshToken.result.access_token) {
            return {
                'setValue':{
                    accessToken:{data:refreshToken.result.access_token},
                    refreshToken:{data:refreshToken.result.refresh_token},
                    getTokenMsg:{data:'OK'}
                }
            };
        }else{
           return {'setValue':{
               getTokenMsg:{data:refreshToken.result.error}
             }}
        }
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
    if (apiResponse.httpCode === 401 || (apiResponse.result && apiResponse.result.code === 'INVALID_TOKEN')) {
        try{
            let refreshToken = invoker.invokeWithoutIntercept("refreshToken");
            if(refreshToken && refreshToken.result &&refreshToken.result.access_token){
                return {"accessToken": refreshToken.result.access_token};
            }
        }catch (e) {
            log.warn(e)
        }
    }
    return null;
}