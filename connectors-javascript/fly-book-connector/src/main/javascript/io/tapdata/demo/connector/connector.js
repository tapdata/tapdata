function discover_schema(connectionConfig) {
    return ['example_table'];
}
function connection_test(connectionConfig) {
    let result = invoker.invokeV2("GetAppToken");
    return [{"TEST": "Test App ID and App Secret", "CODE": result.result.code === 0 ? 1 : -1 , "RESULT": result.result.code === 0 ? "Pass" : "those App ID and App Secret are invalid. "}] ;
}
function write_record(connectionConfig, nodeConfig, eventDataList) {
    for (let index = 0; index < eventDataList.length; index++){
        let event = eventDataList[index];
        if ('undefined' == event.phone || null == event.phone) log.error('Receive user\'s open id cannot be empty.please make sure param [receiveId] is useful.');
        let receiveIdData = invoker.invoke("GetOpenIdByPhone",{'userPhone': [event.phone]});
        let receiveId = receiveIdData.result.data.user_list[0].user_id;
        if ('undefined' == receiveId || null == receiveId){            //用户：{{phone}}, 这位用户不在应用的可见范围中，请确保应用的此用户在当前版本下可见，您可在应用版本管理与发布中查看最新版本下的可见范围，如有必要请在创建新的版本并将此用户添加到可见范围。
            log.warn(' User: '+ phone +', this user is not in the visible range of the application. Please ensure that this user of the application is visible under the current version. You can view the visible range under the latest version in the application version management and release. If necessary, create a new version and add this user to the visible range. ' + ',message is: '+ connect);
            continue;
        }
        if ('undefined' == event.connect || null == event.connect) log.error('receive message cannot be empty. please make sure param [connect] is useful.');
        invoker.invoke("flyBookSendMessage",{"content": event.connect, "receive_id": receiveId});
    }
    return true;
}
function update_token(connectionConfig, nodeConfig, apiResponse) {
    if (apiResponse.result.data.code !== 99991663 && apiResponse.result.data.code !== 99991661) return null;
    let result = invoker.invokeV2("GetAppToken");
    if (result.result.code === 0) return {"token": result.result.tenant_access_token};
    else log.error('Cannot get tenant access token, please check your app_id or app_secret or check api named GetAppToken. ');
}