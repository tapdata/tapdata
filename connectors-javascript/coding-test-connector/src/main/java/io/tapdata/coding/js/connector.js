var codingAPI = tapAPI.loadAPI();
function discover_schema(connectionConfig){
    return ['Issues'];
}
function batch_read(connectionConfig, nodeConfig, offset, table, pageSize, batchReadSender){
    if(!offset) offset = {"pageFrom": 1,"pageSize": pageSize};
    codingAPI.iterateAllData("TAP_TABLE[Issues](PAGE_SIZE_PAGE_INDEX:Response.Data.List)获取事项列表", offset, (result, offset, error) => {
        offset.pageFrom = offset.pageFrom  + 1;
        batchReadSender.send(result.data, result.hasNext, offsetState);
    });
    return "batch_read";
}
function write_record(connectionConfig, nodeConfig, table, records){
    return "write_record";
}
function stream_read(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender){
    offset = !offset ? {"startData":new Date(),"page": 1,"size": pageSize} : {"startData":offset.timestamp,"page": 1,"size": pageSize};
    codingAPI.iterateAllData("TAP_TABLE[Issues](PAGE_SIZE_PAGE_INDEX:Response.Data.List)获取事项列表", offset, (result, offset, error) => {
        offset.page = offset.page + 1;
        streamReadSender.send(result.data, result.hasNext, offsetState);
    });
}
function connection_test(connectionConfig){
    return [{
        "TEST":"hello",
        "CODE": 1,
        "RESULT":""
    }];
}
function batch_count(connectionConfig,nodeConfig,table){
    return 2;
}
function expire_status(){
    return [{"httpCode" : 401},{"header" : {"Code":401}},{"body" : {"code":200,"msg":"out of token"}}];
}
function update_token(connection){
    codingAPI.post("{{baseUrl}}/access_token");
    return {"access_token":token.data};
}