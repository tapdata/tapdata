function discover_schema(connectionConfig) {
    let app = invoker.invoke("AppInfo");
    let appName = app.data.app.app_name;
    return [appName + '_向群组或用户发送消息'];
}

function connection_test(connectionConfig) {
    let result = invoker.invokeWithoutIntercept("GetAppToken");
    return [{
        "test": "Test App ID and App Secret",
        "code": result.result.code === 0 ? 1 : -1,
        "result": result.result.code === 0 ? "Pass" : "those App ID and App Secret are invalid. "
    }];
}

var receiveOpenIdMap = {};

function insert_record(connectionConfig, nodeConfig, eventDataList) {
    let succeedDataArr = [];
    //let sendConfig = nodeConfig.sendItemConfig;
    for (let index = 0; index < eventDataList.length; index++) {
        let event = eventDataList[index].after_data;
        // 消息类型（默认text）：text、post、image、interactive、share_chat、share_user、audio、media、file、sticker
        // https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/im-v1/message/create_json#7215e4f6
        let contentType = event.type;
        let receivedUser = event.phoneOrEmail;
        if ('undefined' === contentType || null == contentType) {
            log.warn('Message type cannot be empty, it will be text now, you must be know message type is only be one of [text | post | image | interactive | share_chat | share_user | audio | media | file | sticker].');
            contentType = 'text';
        }
        if ('undefined' === receivedUser || null == receivedUser) {
            log.error('Receive user\'s phone or email cannot be empty.please make sure param [phoneOrEmail] is useful which is receive msg user\'s phone or email.');
        }
        let receiveId = receiveOpenIdMap[receivedUser];
        if ('undefined' === receiveId || null == receiveId) {
            let receiveIdData = invoker.invoke("GetOpenIdByPhone", { 'userMobiles':  receivedUser , "userEmails": receivedUser });
            receiveId = receiveIdData.result.data.user_list[0].user_id;
            if ('undefined' === receiveId || null == receiveId) {
                // 用户：{{phoneOrEmail}}, 这位用户不在应用的可见范围中，
                // 请确保应用的此用户在当前版本下可见，您可在应用版本管理与发布中查看最新版本下的可见范围，如有必要请在创建新的版本并将此用户添加到可见范围。
                receiveId = receiveIdData.result.data.user_list[1].user_id;
            }
            if (undefined === receiveId || null == receiveId) {
                log.warn(' User: {}, this user is not in the visible range of the application. Please ensure that this user of the application is visible under the current version. You can view the visible range under the latest version in the application version management and release. If necessary, create a new version and add this user to the visible range, message is: {}', event.phone, event.content);
                continue;
            }
            receiveOpenIdMap[event.phone] = receiveId;
        }
        if ('undefined' === event.content || null == event.content) log.error('Receive message cannot be empty. please make sure param [connect] is useful.');
        let writeResult;
        try {
            writeResult = invoker.invoke("flyBookSendMessage", {
                "content": event.content,
                "receive_id": receiveId,
                "msg_type": contentType
            });
        } catch (e) {
            throw e;
        }
        if (writeResult.result.code === 0) succeedDataArr.push(eventDataList[index]);
        else {
            log.warn(writeResult.result.msg);
        }
    }
    return succeedDataArr;
}

function update_token(connectionConfig, nodeConfig, apiResponse) {
    if (apiResponse.result.code != 99991663 && apiResponse.result.code != 99991661) return null;
    let result = invoker.invokeWithoutIntercept("GetAppToken");
    if (result.result.code === 0) return {"token": result.result.tenant_access_token};
    else log.error('Cannot get tenant access token, please check your app_id or app_secret or check api named GetAppToken. ');
}