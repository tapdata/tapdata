var coding_schema = ['Issues'];
function discover_schema(connectionConfig) {
    return coding_schema;
}
function batch_read(connectionConfig, nodeConfig, offset, table, pageSize, batchReadSender) {
    offset = {"startData": new Date(), "PageNumber": 1, "PageSize": 500} ;
    iterateAllData("Issues", offset, (result, offsetNext, error) => {
        offsetNext.PageNumber = offsetNext.PageNumber + 1;
        batchReadSender.send(result.Response.Data.List, offsetNext);
        return result.Response.Data.PageNumber < result.Response.Data.TotalPage;
    });
}
function stream_read(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {
    offset = OptionalUtil.isEmpty(offset) ? {"startData": new Date(), "PageNumber": 1, "PageSize": 500} : offset;
    iterateAllData("Issues", offset, (result, offsetNext, error) => {
        offsetNext.PageNumber = offsetNext.PageNumber + 1;
        streamReadSender.send(result.Response, offsetNext, true);
        return result.Response.Data.PageNumber < result.Response.Data.TotalPage;
    });
}
function connection_test(connectionConfig) {
    var issuesList = invoker.invoke(coding_schema[0]);
    return [{
        "TEST": " Check whether the interface call passes of Issues List API.",
        "CODE": issuesList ? 1 : -1,
        "RESULT": issuesList ? "Pass" : "Not pass"
    }];
}