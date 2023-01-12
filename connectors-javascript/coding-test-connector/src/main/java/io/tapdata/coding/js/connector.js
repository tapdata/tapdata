function discover_schema(connectionConfig) {
    return ['Issues'];
}
function batch_read(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    if (!isParam(offset) || null == offset) offset = {PageNumber: 1, PageSize: 500,SortKey:'CREATED_AT',SortValue:'ASC',Conditions:[{Key: 'CREATED_AT',Value: '1000-01-01_' + nowDate()}]} ;
    read('Issues',offset,batchReadSender,false);
}
function stream_read(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {
    if (!isParam(offset) || null == offset) offset = {PageNumber: 1, PageSize: 500,SortKey:'UPDATED_AT',SortValue:'ASC',Conditions:[{Key: 'UPDATED_AT',Value: '1000-01-01_' + nowDate()}]} ;
    let condition = offset.Conditions.find(ele => typeof(ele) != 'undefined');
    offset.Conditions = [{Key:"UPDATED_AT",Value: isParam(condition) && null != condition ? condition.Value.split('_').shift() + nowDate():'1000-01-01_' + nowDate()}];
    offset.SortKey = "UPDATED_AT";
    read('Issues',offset,streamReadSender,true);
}
function connection_test(connectionConfig) {
    let issuesList = invoker.invoke('Issues');
    return [{
        "TEST": " Check whether the interface call passes of Issues List API.",
        "CODE": issuesList ? 1 : -1,
        "RESULT": issuesList ? "Pass" : "Not pass"}];
}
function read(urlName, offset, sender, isStreamRead){
    iterateAllData(urlName, offset, (result, offsetNext, error) => {
        offsetNext.PageNumber = ( !isStreamRead || (isStreamRead && result.Response.Data.TotalPage > result.Response.Data.PageNumber )) ? offsetNext.PageNumber + 1 : 1;
        let condition = offset.Conditions.find(ele => typeof(ele) != 'undefined');
        if(isStreamRead && isParam(result.Response.Data.List) && result.Response.Data.TotalPage <= result.Response.Data.PageNumber) offset.Conditions = [{Key:condition.Key,Value: formatDate(result.Response.Data.List[result.Response.Data.List.length-1].UpdatedAt) + '_' + nowDate()}];
        sender.send(result.Response.Data.List, offsetNext, isStreamRead);
        return result.Response.Data.PageNumber < result.Response.Data.TotalPage;
    });
}