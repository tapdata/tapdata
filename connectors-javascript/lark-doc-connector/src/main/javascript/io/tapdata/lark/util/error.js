var apiError = {
    check: function (code) {
        let asLine = apiError.checkAsLine(code);
        if (null == asLine){
            return false;
        }
        log.warn(asLine);
        return true;
    },
    checkAsLine: function (code){
        if ('undefined' === code || null === code) {
            return null;
        }
        let element = apiError.config["" + code];
        if (undefined === element || null === element) {
            return stringUtils.format("Lark error from: error code is {}, the HTTP request is incorrect. Please check the {} and self-inspect the defect.", [code, "https://open.feishu.cn/document/ukTMukTMukTM/ugjM14COyUjL4ITN"]);
        } else {
            return stringUtils.format("Lark error from: error code is {}, the HTTP request is incorrect. And the error message is: {}, {}.",[ code, element.msg, element.zh]);
        }
    },
    config: {
        "230001": {
            "msg": "Your request contains an invalid request parameter.Please refer to: https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/im-v1/message/create_json ",
            "zh": "参数错误，请根据接口返回的错误信息并参考文档检查输入参数"
        },
        "230002": {
            "msg": "The bot can not be outside the group",
            "zh": "机器人不在对应群组中"
        },
        "230006": {
            "msg": "Bot ability is not activated",
            "zh": "机器人能力未启用 。在开发者后台-应用功能-机器人页面开启机器人功能并发布上线"
        },
        "230013": {
            "msg": "Bot has NO availability to this user",
            "zh": "机器人对用户没有可用性。可在开发者后台-应用发布-版本管理与发布 编辑应用对用户的可用性并发布"
        },
        "230015": {
            "msg": "P2P chat can NOT be shared",
            "zh": "私聊会话不允许被分享"
        },
        "230017": {
            "msg": "Bot is NOT the owner of the resource",
            "zh": "机器人不是资源的拥有者"
        },
        "230018": {
            "msg": "These operations are NOT allowed at current group settings",
            "zh": "当前操作被群设置禁止，请检查群设置或联系群管理员"
        },
        "230019": {
            "msg": "The topic does NOT exist",
            "zh": "当前话题不存在"
        },
        "230020": {
            "msg": "This operation triggers the frequency limit",
            "zh": "当前操作触发限频，请降低请求频率"
        },
        "230022": {
            "msg": "The content of the message contains sensitive information",
            "zh": "消息包含敏感信息，请检查消息内容"
        },
        "230025": {
            "msg": "The length of the message content reaches its limit",
            "zh": "消息体长度超出限制。文本消息请求体最大不能超过150KB；卡片及富文本消息请求体最大不能超过30KB"
        },
        "230027": {
            "msg": "Lack of necessary permissions",
            "zh": "请根据本文档中的权限要求部分补充所需权限"
        },
        "230099": {
            "msg": "Failed to create card content",
            "zh": "创建卡片失败，失败原因请查看接口报错信息"
        },
        "230028": {
            "msg": "The messages do NOT pass the audit",
            "zh": "消息DLP审查未通过，当消息内容中含有明文电话号码、明文个人邮箱等内容时可能会触发该错误；请根据接口返回的错误信息检查消息内容"
        },
        "230029": {
            "msg": "User has resigned",
            "zh": "用户已离职"
        },
        "230034": {
            "msg": "The receive_id is invalid",
            "zh": "请求参数中的receive_id不合法，请检查"
        },
        "230035": {
            "msg": "Send Message Permission deny",
            "zh": "没有发言权限 ，请检查机器人是否在该群内，或群是否已开启禁言"
        },
        "230036": {
            "msg": "Tenant crypt key has been deleted",
            "zh": "租户加密密钥已被删除，请联系企业管理员"
        },
        "230038": {
            "msg": "Cross tenant p2p chat operate forbid",
            "zh": "跨租户的单聊不允许通过本接口发送消息"
        },
        "230049": {
            "msg": "The message is being sent",
            "zh": "消息正在发送中，请稍后"
        },
        "230053": {
            "msg": "The user has stopped the bot from sending messages",
            "zh": "用户已设置不再接收机器人消息，无法主动给用户发送单聊消息"
        }
    }
}