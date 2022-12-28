var qingFlowAPI = tapAPI.loadAPI();
function discover_schema(connectionConfig){
    var a = 10;
    var b = a + 10;
    var c = b++;
    var sum = a + b + c ;
    tapLog.warn("","");
    //return ['Table1','Table2','Table3'];
    /**
     * return [
     * {
     *     "name":"Table1"
     *     "fields":{
     *          "key1":{
     *              "type":"String"
     *          },
     *          "key2":{
     *              "type":"String"
     *          }
     *      }
     * },
     * ['Table2','Table3','Table4'],
     * 'Table6'
     * ]
     *
     * */
    return [
        {
            "name":"User",
            "fields":{
                "name":{
                    type:"String",
                    default:"111",
                    comment:"用户名称"
                },
                "scope":{
                    type:"Number",
                    default:10,
                    comment:"得分"
                }
            },
            "comment":"用户信息表"
        },
        {
            "fields":{
                "key1":{
                    type:"String",
                    default:"111"
                },
                "key2":{
                    type:"Number",
                    default:10
                }
            },
            "comment":"",
            "name":"Pet"
        },
        "Dog"
    ];
}
function batch_read(connectionConfig, nodeConfig, offset, table, pageSize, batchReadSender){
    if(!offset) offset = {"pageFrom": 1,"pageSize": pageSize};
    // feishuAPI.iterateAllData("{{base_url}}/list", offset, (result, offset, error) => {
    //     offset.pageFrom = offset.pageFrom  + 1;
    //     batchReadSender.send(result.data, result.hasNext, offsetState);
    // });
    return "batch_read";
}
function write_record(connectionConfig, nodeConfig, table, records){
    return "write_record";
}
function stream_read(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender){
    offset = !offset ? {"startData":new Date(),"page": 1,"size": pageSize} : {"startData":offset.timestamp,"page": 1,"size": pageSize};
    // feishuAPI.iterateAllData("{{base_url}}/list?page=:page", offset, (result, offset, error) => {
    //     offset.page = offset.page + 1;
    //     streamReadSender.send(result.data, result.hasNext, offsetState);
    // });
    return 1;
}

/**
 * testResult = [
 *      {
 *          "TEST":"Test Api 1 which is the test item' title. ",
 *          "CODE":0,//default0(0:warn,1:succeed,-1:error),
 *          "RESULT":"there is a warn message for this test item."
 *      }
 * ]
 * */
function connection_test(connectionConfig){
    return {
        "key":"hello",
        "value":"connection_test"
    };
}

/**
 * Tap Function -- table_count
 * You need to return the number of tables through this method
 * @return Number
 * */
function table_count(connectionConfig){
    return 3;
}

function batch_count(connectionConfig,nodeConfig,table){
    return 2;
}

/**@description token过期状态描述，由开发者针对数据源特性进行描述，描述为列表类型，支持多个或关系的描述，每个描述都是且的关系*/
function expire_status(){
    return [{"httpCode" : 401},{"header" : {"Code":401}},{"body" : {"code":200,"msg":"out of token"}}];
}

/**@description token 过期的操作方式，开发者需要手动实现token过期后获取新的access_token*/
function update_token(connection){
    var token = {"data":0};//feishuAPI.post("{{baseUrl}}/access_token");
    return {"access_token":token.data};
}
// const feishuAPI = tapAPI.load();//使用默认路基以及导出文件中配置的参数
// function batch_read(connection, node, offset, table, pageSize, batchReadSender){
//     var tableNames = discover_schema(connection);
//     for(table in tableNames) {
//         if(!offset) offset = {"pageFrom": 1,"pageSize": pageSize};
//         feishuAPI.iterateAllData("{{base_url}}/list", offset, (result, offset, error) => {
//             offset.pageFrom = offset.pageFrom  + 1;
//             batchReadSender.send(result.data, result.hasNext, offsetState);
//         });
//     }
// }
// function stream_read(connection, node, offset, table, pageSize, streamReadSender){
//     offset = !offset ? {"startData":new Date(),"page": 1,"size": pageSize} : {"startData":offset.timestamp,"page": 1,"size": pageSize};
//     feishuAPI.iterateAllData("{{base_url}}/list?page=:page", offset, (result, offset, error) => {
//         offset.page = offset.page + 1;
//         streamReadSender.send(result.data, result.hasNext, offsetState);
//     });
// }