function discoverSchema(connectionConfig) {
    let app;
    try {
        app = invoker.invoke("AppInfo").result;
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
    }catch (e){
        return ["Lark_IM"];
    }
}

function connectionTest(connectionConfig) {
    let app ;
    try {
        app = invoker.invoke("AppInfo").result;
        let isApp = 1;
        if (!sendMsg.checkParam(app) && !sendMsg.checkParam(app.data) && !sendMsg.checkParam(app.data.app)){
           log.warn("Cannot get Application info, and application name will be empty now.")
            isApp = -1;
        }
        return [{
            "test": "Get App info",
            "code": isApp,
            "result": isApp === 1 ? "App name is:" + app.data.app.app_name : "Can not get App info, please check you App ID and App Secret."
        }];
    }catch (e){
        return [{
            "test":" Input parameter check ",
            "code":-1,
            "result": "Can not get App info, please check you App ID and App Secret."
        }];
    }
}

var receiveOpenIdMap = {};
var sendMsg = new larkSendMsgV2();
function insertRecord(connectionConfig, nodeConfig, eventDataMap) {
    return sendMsg.sendMsg(connectionConfig, nodeConfig, eventDataMap, 'V2');
}

function updateToken(connectionConfig, nodeConfig, apiResponse) {
    if (!sendMsg.checkParam(apiResponse.result.code) ||
        (apiResponse.result.code !== 99991663 && apiResponse.result.code !== 99991661)) {
        if (!sendMsg.checkParam(apiResponse.result.code) && apiResponse.httpCode !== 0){
            apiError.check(apiResponse.result.code);
        }
        return null;
    }
    let result = invoker.invokeWithoutIntercept("GetAppToken");
    if (sendMsg.checkParam(apiResponse.result.code) && result.result.code === 0) {
        return {"token": result.result.tenant_access_token};
    } else {
        log.warn('Cannot get tenant access token, please check your app_id or app_secret or check api named GetAppToken. ');
        return null;
    }
}

function commandCallback(connectionConfig, nodeConfig, commandInfo) {
    let exec = new CommandStage().exec(commandInfo.command);
    if (null != exec) {
        return exec.command(connectionConfig, nodeConfig, commandInfo);
    }
}
