var batchStart = nowDate();

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
    return ['example_table'];
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
function stream_read(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {

}


/**
 * @return The returned result is not empty and must be in the following form:
 *          [
 *              {"test": String, "code": Number, "result": String},
 *              {"test": String, "code": Number, "result": String},
 *              ...
 *          ]
 *          param - test :  The type is a String, representing the description text of the test item.
 *          param - code :  The type is a Number, is the type of test result. It can only be [-1, 0, 1], -1 means failure, 1 means success, 0 means warning.
 *          param - result : The type is a String, descriptive text indicating test results.
 * @param connectionConfig  Configuration property information of the connection page
 * */
function connection_test(connectionConfig) {
    return [{
        "test": "Example test item",
        "code": 1,
        "result": "Pass"
    }];
}


/**
 *
 * @param connectionConfig
 * @param nodeConfig
 * @param commandInfo
 * */
function command_callback(connectionConfig, nodeConfig, commandInfo) {

}

/**
 * @param connectionConfig
 * @param nodeConfig
 * @param tableNameList
 * @param eventDataMap  eventDataMap.data is sent from WebHook
 *
 * @return array with data maps.
 *  Each data includes five parts:
 *      - event_type : event type ,only with : i , u, d. Respectively insert, update, delete;
 *      - table_name : Data related table;
 *      - reference_time : Time stamp of event occurrence;
 *      - after_data : After the event, only after_data can be added or deleted as a result of the data
 *      - before_data : Before the event, the result of the data will be before_data only if the event is modified
 *  please return with: [
 *      {
 *          "event_type": "i/u/d",
 *          "table_name": "${example_table_name}",
 *          "reference_time": Number(),
 *          "after_data": {},
 *          "before_data":{}
 *      },
 *      ...
 *     ]
 * */
function web_hook_event(connectionConfig, nodeConfig, tableNameList, eventDataMap) {

    //return [
    //     {
    //         "event_type": "i/u/d",
    //         "table_name": "${example_table_name}",
    //         "reference_time": Number(),
    //         "after_data": {},
    //         "before_data":{}
    //     }
    //]
}

/**
 * [
 *  {
 *      "event_type": "i/u/d",
 *      "table_name": "${example_table_name}",
 *      "reference_time": Number(),
 *      "after_data": {},
 *      "before_data":{}
 *  },
 *  ...
 * ]
 * @param connectionConfig
 * @param nodeConfig
 * @param eventDataList type is js array with data maps.
 *  Each data includes five parts:
 *      - event_type : event type ,only with : i , u, d. Respectively insert, update, delete;
 *      - table_name : Data related table;
 *      - reference_time : Time stamp of event occurrence;
 *      - after_data : After the event, only after_data can be added or deleted as a result of the data
 *      - before_data : Before the event, the result of the data will be before_data only if the event is modified
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
    // if (apiResponse.code === 401) {
    //     let result = invoker.invokeV2("apiName");
    //     return {"access_token": result.result.token};
    // }
    // return null;
}