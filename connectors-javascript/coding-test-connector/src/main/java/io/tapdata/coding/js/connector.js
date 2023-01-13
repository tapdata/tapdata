var batchStart = nowDate();
function discover_schema(connectionConfig) {
    return ['Issues'];
}
function batch_read(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    if (!isParam(offset) || null == offset || typeof(offset) != 'object' ) offset = {PageNumber: 1, PageSize: 500,SortKey:'CREATED_AT',SortValue:'ASC',Conditions:[{Key: 'CREATED_AT',Value: '1000-01-01_' + batchStart}]} ;
    read('Issues',offset,batchReadSender,false);
}
function stream_read(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {
    if (!isParam(offset) || null == offset || typeof(offset) != 'object') offset = {PageNumber: 1, PageSize: 500,SortKey:'UPDATED_AT',SortValue:'ASC',Conditions:[{Key: 'UPDATED_AT',Value: batchStart + '_' + nowDate()}]} ;
    let condition = firstElement(offset.Conditions);
    offset.Conditions = [{Key:"UPDATED_AT",Value: isParam(condition) && null != condition ? firstElement(condition.Value.split('_')) + '_' + nowDate(): batchStart + '_' + nowDate()}];
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
        offsetNext.PageNumber = ( !isStreamRead || (isStreamRead && result.Response.Data.TotalPage >= result.Response.Data.PageNumber )) ? offsetNext.PageNumber + 1 : 1;
        if(isStreamRead && isParam(result.Response.Data.List) && result.Response.Data.TotalPage < result.Response.Data.PageNumber)
            offset.Conditions = [{Key:'UPDATED_AT',Value: formatDate(result.Response.Data.List[result.Response.Data.List.length-1].UpdatedAt) + '_' + nowDate()}];
        sender.send(result.Response.Data.List, offsetNext, isStreamRead);
        return offsetNext.PageNumber <= result.Response.Data.TotalPage && isAlive();
    });
}