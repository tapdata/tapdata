class larkSendMsg {
    writerResultCollector;
    dataConvertConfig = {
        "receiveType": "receiveType",
        "receiveId": "receiveId",
        "contentType": "contentType",
        "content": "content"
    };
    markSentResult(event){
        if ('undefined' === this.writerResultCollector || null == this.writerResultCollector){
            return;
        }
        this.writerResultCollector.markSent(event);
    }

    sendMsg(connectionConfig, nodeConfig, event) {
        let data = this.convertEventAndSend(event, this.dataConvertConfig);
        if (null != data) {
            let r = this.sendHttp(data, event.afterData);
            if (!this.checkParam(r)) {
                throw("Error send message")
            }
        }
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
            log.error('Receive type is user or email or phone or chat,it cannot be empty.please make sure param [receiveType] is useful.');
        }
        if (!this.checkParam(receivedUser)) {
            log.error('Receive id is chat_id or user\'s phone or email or open_id,it cannot be empty.please make sure param [receiveId] is useful.');
        }
        let receiveId = this.getUserId(receiveType, receivedUser);
        if (!this.checkParam(receiveId)) {
            return null;
        }
        if (!this.checkParam(content)) log.error('Receive message cannot be empty. please make sure param [connect] is useful.');
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
                    receiveId = receiveIdData.result.data.user_list[0].user_id;
                    if (!this.checkParam(receiveId)) {
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
        let writeResult;
        try {
            writeResult = invoker.invoke("flyBookSendMessage", sendData);
            if ((writeResult.httpCode >= 200 || writeResult.httpCode < 300) && writeResult.result.code === 0) {
                return true;
            } else {
                log.warn("A message failed to be sent. The following data was not sent successfully: {}, and error message is : {}, http code is: {}.", historyData, writeResult.result.msg, writeResult.httpCode);
                return false;
            }
        } catch (e) {
            log.warn("A message failed to be sent. The following data was not sent successfully: {}, error message: {}", historyData, e.message);
            return false;
        }
    }

    checkParam(param) {
        return 'undefined' !== param && null != param;
    }
}