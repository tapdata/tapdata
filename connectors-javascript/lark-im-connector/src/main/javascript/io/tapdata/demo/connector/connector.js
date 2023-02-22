config.setStreamReadIntervalSeconds(600);

function discoverSchema(connectionConfig) {
    let app = invoker.invoke("AppInfo").result;
    let appName = app.data.app.app_name;
    return [{
        'name': appName + '_向群组或用户发送消息',
        'fields': {
            'receiveType': {
                'type': 'String',
                'comment': 'user | email | phone | chat, it is used to represent the type of message receiver.',
                'nullable': true
            },
            'receiveId': {
                'type': 'String',
                'comment': 'user_open_id | user_email | user_phone | chat_id, it is the message receiver，APP sends messages to specified users or group chat through this field.',
                'nullable': true
            },
            'contentType': {
                'type': 'String',
                'comment': 'contentType contain text | post | image | interactive | share_chat | share_user | audio | media | file | sticker，default is text.\n' +
                    '  For specific message types, see the description on the official document: https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/im-v1/message/create_json',
                'nullable': true
            },
            'content': {
                'type': 'String',
                'comment': ' Message content body ',
                'nullable': true
            }
        }
    }];
}

function connectionTest(connectionConfig) {
    let app = invoker.invoke("AppInfo").result;
    let isApp = 'undefined' !== app && null != app && 'undefined' !== app.data && null != app.data && 'undefined' !== app.data.app;
    let testItem = [{
        "test": "Get App info",
        "code": isApp ? 1 : -1,
        "result": isApp ? "App name is:" + app.data.app.app_name : "Can not get App info, please check you App ID and App Secret."
    }];
    // if (isApp){
    //     let needAuthority = {
    //         "contact:user.id:readonly":'通过手机号或邮箱获取用户 ID',
    //         "contact:user.employee_id:readonly":'获取用户 user ID',
    //         "im:message":'获取与发送单聊、群组消息',
    //         "im:message:send_as_bot":'以应用的身份发消息'
    //     }
    //     let appScopes = app.data.app.scopes;
    //     let hasEnoughAuthority = true;
    //     let notSupport = '';
    //     for (let index=0;index<needAuthority.length;index++){
    //         let s = needAuthority[index];
    //         let newVar = needAuthority.get(s.scope);
    //         if ('undefined' === newVar || null == newVar){
    //             if (hasEnoughAuthority) {
    //                 hasEnoughAuthority = false;
    //             }
    //             notSupport += s.description + ":" + s.scope + ";";
    //         }
    //     }
    //     testItem.push({
    //         "test": "Check whether the authority is complete ",
    //         "code": hasEnoughAuthority ? 1 : -1,
    //         "result": hasEnoughAuthority ? "Permission configuration meets the requirements" : "The following permissions are configured. Please configure and try again: " + notSupport;
    //     })
    // }
    return testItem;
}

var receiveOpenIdMap = {};

function insertRecord(connectionConfig, nodeConfig, eventDataList, sender) {
    let sendMsg = new larkSendMsgV2();
    sendMsg.writerSender = sender;
    return sendMsg.sendMsg(connectionConfig, nodeConfig, eventDataList, 'V2');
}

function updateToken(connectionConfig, nodeConfig, apiResponse) {
    if (apiResponse.result.code !== 99991663 && apiResponse.result.code !== 99991661) return null;
    let result = invoker.invokeWithoutIntercept("GetAppToken");
    if (result.result.code === 0) return {"token": result.result.tenant_access_token};
    else log.error('Cannot get tenant access token, please check your app_id or app_secret or check api named GetAppToken. ');
}

/**
 *
 * @param connectionConfig
 * @param nodeConfig
 * @param commandInfo
 *
 *  "commandInfo": {
 *      "command": String,   //command类型
 *      "action": String,    //action类型
 *      "argMap": Object,    //查询参数
 *      "time": Number       //command发起时间
 *  }
 * */
function commandCallback(connectionConfig, nodeConfig, commandInfo) {
    //
    let commandName = commandInfo.command;
    let exec = new CommandStage().exec(commandInfo.command);
    if (null != exec) {
        return exec.command(connectionConfig, nodeConfig, commandInfo);
    }
}
