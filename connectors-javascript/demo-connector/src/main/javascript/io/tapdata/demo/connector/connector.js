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
        "CODE": 1 ,
        "RESULT": "Pass"
    }];
}


/**
 *
 * @param connectionConfig
 * @param nodeConfig
 * @param commandInfo
 * */
function command_callback(connectionConfig, nodeConfig, commandInfo){

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