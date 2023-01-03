var coding_schema = ['Issues'];
var codingAPI;
function discover_schema(connectionConfig){
    debugger;
    return coding_schema;
}
function batch_read(connectionConfig, nodeConfig, offset, table, pageSize, batchReadSender){
    codingAPI = tapAPI.loadAPI();
    offset.id = "0";
    if(!offset) offset = {"PageNumber": 1,"PageSize": pageSize};
    codingAPI.iterateAllData("GetIssuesList", offset, (result, offsetNext, error) => {
        offsetNext.PageNumber = offsetNext.PageNumber  + 1;
        batchReadSender.send(result.data, result.hasNext, offsetNext);
    });
}
function stream_read(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender){
    codingAPI = tapAPI.loadAPI();
    offset = !offset ? {"startData":new Date(),"PageNumber": 1,"PageSize": pageSize} : {"startData":offset.timestamp,"PageNumber": 1,"PageSize": pageSize};
    codingAPI.iterateAllData("GetIssuesList", offset, (result, offsetNext, error) => {
        offsetNext.PageNumber = offsetNext.PageNumber + 1;
        streamReadSender.send(result.data, result.hasNext, offsetNext,true);
    });
}
function connection_test(connectionConfig){
    codingAPI = tapAPI.loadAPI();
    var issuesList = codingAPI.invoke("GetIssuesList");
    return [{
        "TEST":" Check whether the interface call passes of Issues List API.",
        "CODE": issuesList? 1 : -1,
        "RESULT": issuesList? "Pass" : "Not pass"
    }];
}
function batch_count(connectionConfig,nodeConfig,table){
    codingAPI = tapAPI.loadAPI();
    var issuesList = codingAPI.invoke("GetIssuesList");
    return issuesList ? 0 : issuesList.Response.Data.TotalCount;
}
// function write_record(connectionConfig, nodeConfig, table, records){
//     return {
//         "data":{
//             "id":100,
//             "name":"gavin",
//             "type":{
//                 "id":10,
//                 "default":false,
//                 "name":"write_record"
//             }
//         }
//     };
// }
function testString(){
    log.warn("error");
    return "String";
}
function testChar(){
    return 'a';
}
function emptyArr(){
    return [];
}
function emptyMap(){
    return {};
}
function testMap(){
    return {"key":10,"value":"va","arr":["a",10,"hhse"]};
}
function testBool(){
    return false;
}
function testNumber(){
    return 10;
}
function testFolat(){
    return 12.6;
}
function testNull(){
    return null;
}
function testVoid(){

}
function testArrMap(){
    return [
        {
            id:2,
            name:"kit"
        },
        {
            "id":1
        }
    ];
}