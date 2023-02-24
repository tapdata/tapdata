class larkSendMsgV2 extends larkSendMsg {
    sendMsg(connectionConfig, nodeConfig, eventDataMap) {
        let argument = arguments[3];
        if ('undefined' === argument || null == argument || argument === 'V1') {
            super.sendMsg(connectionConfig, nodeConfig, eventDataMap);
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
        return super.sendMsg(connectionConfig, nodeConfig, event);
    }

    appointSend(connectionConfig, nodeConfig, event) {
        if (!this.appointSendWithData(connectionConfig, nodeConfig, event)) {
            throw("Error send message")
        }
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
                //backArr.push(eData);
                throw("Error send message, data: " + JSON.stringify(eData.afterData));
            }
        }

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
            "content": typeof (content) == 'string' ? content.replaceAll('"', "\"") : ("" + JSON.stringify(content).replaceAll('"', '\"')),
            "receive_id": receiveId,
            "msg_type": contentType,
            "receive_id_type": "chat" === receiveType ? 'chat_id' : 'open_id'
        };
    }
}