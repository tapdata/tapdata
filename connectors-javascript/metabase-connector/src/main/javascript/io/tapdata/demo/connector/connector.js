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

    /**
     * 已实现discover_schema
     * **/
    let sessionToken = invoker.invoke('TAP_GET_TOKEN session api');

    let invoke = invoker.invoke('TAP_TABLE[allCard](PAGE_NONE)allCard',
        {"sessionToken": sessionToken.result.id});

    let tableList = [];
    for (let index = 0; index < invoke.result.length; index++) {
        // let table = {
        //     "name": ""+invokeKey.id,
        //     "fields":{
        //         "客户":{
        //             "type":"string",
        //             "default":"xxx"
        //         },
        //         "COUNT":{
        //
        //         }
        //     }
        // }
        tableList.push(invoke.result[index].name + "_" + invoke.result[index].id);
    }
    return tableList;
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
    /**
     *还没进入js的batch_read就引擎就开始报错。
     * **/
    let sessionToken = invoker.invoke('TAP_GET_TOKEN session api', {});
    let data = invoker.invoke('TAP_TABLE[allCard](PAGE_NONE)allCard',
        {"sessionToken": sessionToken.result.id});
    let id = tableName.split("_")[1];
    let thisCard = {};

    for (let index = 0; index < data.result.length; index++) {
        if (data.result.id == id) {
            thisCard = data.result;
            break;
        }
    }
    let invoke = invoker.invoke(
        'TAP_TABLE[queryExportFormat](PAGE_NONE:data)queryExportFormat',
        {"card-id": id});
    let resut = []
    for (let index = 0; index < invoke.result.length; index++) {
        invoke[index].put("Question Name", thisCard.name);
        invoke[index].put("Question ID", thisCard.id);
        invoke[index].put("Current Date", nowDate());
        resut.push(invoke[index]);
    }
    batchReadSender.send(resut, {}, false);
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
// function stream_read(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {
//
// }


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
    let sessionToken = invoker.invoke('TAP_GET_TOKEN session api');
    let invoke = invoker.invoke('TAP_TABLE[allCard](PAGE_NONE)allCard',
        {"sessionToken": sessionToken.result.id});
    return [{
        "TEST": " Check the account read database permission. ",
        "CODE": invoke ? 1 : -1,
        "RESULT": invoke ? "Pass" : "Not pass"
    }];
}


/**
 *
 * @param connectionConfig
 * @param nodeConfig
 * @param commandInfo
 * */
// function command_callback(connectionConfig, nodeConfig, commandInfo) {
//
// }