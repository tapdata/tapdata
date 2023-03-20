class larkSendMsg {
    writerResultCollector;
    dataConvertConfig = {
        "receiveType": "receiveType",
        "receiveId": "receiveId",
        "contentType": "contentType",
        "content": "content"
    };

    markSentResult(event) {
        if ('undefined' === this.writerResultCollector || null == this.writerResultCollector) {
            return;
        }
        this.writerResultCollector.markSent(event);
    }

    sendMsg(connectionConfig, nodeConfig, event) {
        let data = this.convertEventAndSend(event, this.dataConvertConfig);
        if (null != data) {
            return this.sendHttp(data, event.afterData);
        }
        return false;
    }

    convertEventAndSend(eventData, convertConfig) {
        let event = eventData.afterData;
        // 消息类型（默认text）：text、post、image、interactive、share_chat、share_user、audio、media、file、sticker
        // https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/im-v1/message/create_json#7215e4f6
        let contentType = event[convertConfig.contentType];
        let receiveType = event[convertConfig.receiveType];
        let receivedUser = event[convertConfig.receiveId];
        let content = event[convertConfig.content];
        if (!this.checkParam(contentType)) {
            log.warn('Message type cannot be empty, it will be text now, you must be know message type is only be one of [text | post | image | interactive | share_chat | share_user | audio | media | file | sticker].');
            contentType = 'text';
        }
        if (!this.checkParam(receiveType)) {
            log.warn('Receive type is user or email or phone or chat,it cannot be empty.please make sure param [receiveType] is useful.');
            return null;
        }
        if (!this.checkParam(receivedUser)) {
            log.warn('Receive id is chat_id or user\'s phone or email or open_id,it cannot be empty.please make sure param [receiveId] is useful.');
            return null;
        }
        let receiveId = this.getUserId(receiveType, receivedUser);
        if (!this.checkParam(receiveId)) {
            return null;
        }
        if (!this.checkParam(content)) {
            log.warn('Receive message cannot be empty. please make sure param [connect] is useful.');
            return null;
        }
        return {
            "content": content,
            "receive_id": receiveId,
            "msg_type": contentType,
            "receive_id_type": "chat" === receiveType ? 'chat_id' : 'open_id'
        };
    }

    getUserId(receiveType, receivedUser) {
        let receiveId = receiveOpenIdMap[receivedUser];
        switch (receiveType) {
            case 'phone': {
                if (!this.checkParam(receiveId)) {
                    let receiveIdData = invoker.invoke("GetOpenIdByPhone", {
                        "mobiles": receivedUser,
                        "emails": receivedUser
                    });
                    if (!this.checkParam(receiveIdData.result.data)) {
                        log.warn("Cannot get user open id whit use user' phone or email,and the http result data is empty,the phone or email is {}.", receiveId);
                        return null;
                    }
                    let userList = receiveIdData.result.data.user_list;
                    if (!this.checkParam(userList) || userList.length === 0) {
                        log.warn("Cannot get user open id whit use user' phone,and the http result data.user_list is empty,the phone or email is {}.", receiveId);
                        return null;
                    }
                    receiveId = userList[0].user_id;
                    if (!this.checkParam(receiveId)) {
                        if (userList.length < 1) {
                            log.warn("Cannot get user open id whit use user' email,and the http result data.user_list is empty,the phone or email is {}.", receiveId);
                            return null;
                        }
                        // 用户：{{phoneOrEmail}}, 这位用户不在应用的可见范围中，
                        // 请确保应用的此用户在当前版本下可见，您可在应用版本管理与发布中查看最新版本下的可见范围，如有必要请在创建新的版本并将此用户添加到可见范围。
                        receiveId = receiveIdData.result.data.user_list[1].user_id;
                    }
                    if (!this.checkParam(receiveId)) {
                        log.warn('User: {}, this user is not in the visible range of the application. Please ensure that this user of the application is visible under the current version. You can view the visible range under the latest version in the application version management and release. If necessary, create a new version and add this user to the visible range.', receivedUser);
                        return null;
                    }
                    receiveOpenIdMap[receivedUser] = receiveId;
                }
                return receiveId;
            }
            default:
                return receivedUser;
        }
    }

    sendHttp(sendData, historyData) {
        let writeResult = invoker.invoke("flyBookSendMessage", sendData);
        if (writeResult.httpCode < 200 || writeResult.httpCode >= 300) {
            let line = apiError.checkAsLine(writeResult.result.code);
            log.warn("A message failed to be sent. The following data was not sent successfully: {}, {}, {}",
                JSON.stringify(historyData),
                JSON.stringify(writeResult.result),
                 (line == null ? '' : line));
            return false;
        }
        if (!this.checkParam(writeResult.result.code)) {
            let line = apiError.checkAsLine(writeResult.result.code);
            log.warn("A message failed to be sent. The following data was not sent successfully: {}, {}", JSON.stringify(historyData), (line == null ? '' : line));
            return false;
        }
        if (writeResult.result.code === 0) {
            return true;
        } else {
            let line = apiError.checkAsLine(writeResult.result.code);
            log.warn("A message failed to be sent. The following data was not sent successfully: {}, and error message is : {}, http code is: {}, {}",
                JSON.stringify(historyData),
                JSON.stringify(writeResult.result),
                writeResult.httpCode,
                (line == null ? '' : line));
        }
        return false;
    }

    checkParam(param) {
        return 'undefined' !== param && null != param;
    }
}

class larkSendMsgV2 extends larkSendMsg {
    sendMsg(connectionConfig, nodeConfig, eventDataMap) {
        let argument = arguments[3];
        if ('undefined' === argument || null == argument || argument === 'V1') {
            new larkSendMsg().sendMsg(connectionConfig, nodeConfig, eventDataMap);
        }
        let sendType = nodeConfig.sendType;
        switch (sendType) {
            case 'appoint':
                return this.appointSend(connectionConfig, nodeConfig, eventDataMap);
            case 'dynamic_binding':
                return this.dynamicBindingSend(connectionConfig, nodeConfig, eventDataMap);
            default :
                return this.defaultSendType(connectionConfig, nodeConfig, eventDataMap);
        }
    }

    defaultSendType(connectionConfig, nodeConfig, event) {
        return new larkSendMsg().sendMsg(connectionConfig, nodeConfig, event);
    }

    appointSend(connectionConfig, nodeConfig, event) {
        if (!this.appointSendWithData(connectionConfig, nodeConfig, event)) {
            throw("Error send message")
        }
        return true;
    }

    appointSendWithData(connectionConfig, nodeConfig, event) {
        let configOfMessageArr = nodeConfig.messageConfig;
        let msgType = configOfMessageArr[0].messageType;
        let eventData = event.afterData;
        let content = eventData[configOfMessageArr[0].messageField];
        let receivers = nodeConfig.receiver.split(',');
        for (let index = 0; index < receivers.length; index++) {
            let id = receivers[index];
            super.sendHttp({
                "content": content,
                "receive_id": id,
                "msg_type": msgType,
                "receive_id_type": id.startsWith('ou_') ? 'open_id' : 'chat_id'
            }, eventData);
        }
        return true;
    }


    dynamicBindingSend(connectionConfig, nodeConfig, eData) {
        let dynamicBinding = nodeConfig.dynamicBinding;
        let messageConfigArr = nodeConfig.messageConfig;
        for (let index = 0; index < dynamicBinding.length; index++) {
            let dynamicBindingItem = dynamicBinding[index];
            let dataConvertMap = {
                "receiveType": dynamicBindingItem.receiveType,
                "receiveId": dynamicBindingItem.receiveId,
                "contentType": messageConfigArr[0].messageType,
                "content": messageConfigArr[0].messageField
            }

            let sendMsgDataMap = this.convertEventAndSend(eData, dataConvertMap);
            if (!super.sendHttp(sendMsgDataMap, eData.afterData)) {
                return false;
            }
        }
        return true;
    }

    convertEventAndSend(eventData, convertConfig) {
        let event = eventData.afterData;
        // 消息类型（默认text）：text、post、image、interactive、share_chat、share_user、audio、media、file、sticker
        // https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/im-v1/message/create_json#7215e4f6
        let contentType = convertConfig.contentType;
        let receiveType = convertConfig.receiveType;
        let receivedUser = event[convertConfig.receiveId];
        let content = event[convertConfig.content];
        if (!this.checkParam(contentType)) {
            log.warn('Message type cannot be empty, it will be text now, you must be know message type is only be one of [text | post | image | interactive | share_chat | share_user | audio | media | file | sticker].');
            contentType = 'text';
        }
        if (!this.checkParam(receiveType)) {
            log.warn('Receive type is user or email or phone or chat,it cannot be empty.please make sure param [receiveType] is useful.');
            return null;
        }
        if (!this.checkParam(receivedUser)) {
            log.warn('Receive id is chat_id or user\'s phone or email or open_id,it cannot be empty.please make sure param [receiveId] is useful.');
            return null;
        }
        let receiveId = this.getUserId(receiveType, receivedUser);
        if (!this.checkParam(receiveId)) {
            return null;
        }
        if (!this.checkParam(content)){
            log.warn('The message field {} does not meet the format requirements for message body type {}, https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/im-v1/message/create_json#7215e4f6 ', convertConfig.content, contentType);
            return null;
        }
        return {
            "content": typeof (content) == 'string' ? content.replaceAll('"', "\"") : ("" + JSON.stringify(content).replaceAll('"', '\"')),
            "receive_id": receiveId,
            "msg_type": contentType,
            "receive_id_type": "phone" === receiveType ? 'open_id' : receiveType
        };
    }
}