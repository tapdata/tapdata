var invoker = loadAPI();
var userMap = {};

class CreateTask {
    create(connectionConfig, nodeConfig, event) {
        let data;
        try {
            if (nodeConfig.sendType === 'appoint') {
                data = this.convertEventAndCreateTaskV2(event, nodeConfig);
            } else {
                data = this.convertEventAndCreateTask(event);
            }
            if (data === null) {
                return false;
            }
        } catch (e) {
            log.warn("Failed to create a task: {} please check whether the submitted parameters are correct: {}", e, event.afterData);
            return false;
        }
        return this.sendHttp(data);
    }

    convertEventAndCreateTask(eventData) {
        if (!this.checkParam(eventData)) {
            log.warn("eventData is empty, can not be handled")
            return null;
        }
        let event = eventData.afterData;
        if (!this.checkParam(event)) {
            log.warn("afterData is empty, can not be handled");
            return null;
        }

        let richSummary = event.richSummary;
        let richDescription = event.richDescription;
        let collaboratorIds = event.collaboratorIds;
        let time = event.time;
        let followerIds = event.followerIds;
        let title = event.title;
        let url = event.url;

        if (!this.checkParam(richSummary)) {
            log.warn('RichSummary is the title of the task and cannot be empty. ');
            return null;
        }
        if (!this.checkParam(richDescription)) {
            log.warn('RichDescription is the description in the task. It cannot be empty. ');
            return null;
        }
        if (!this.checkParam(collaboratorIds)) {
            log.warn('The collaboratorIds are in charge of the task and cannot be empty. ');
            return null;
        }
        let cUserIds = this.getUserId(collaboratorIds.split(','));
        if (!this.checkParam(cUserIds)) {
            log.warn('The cUserIds are in charge of the task and cannot be empty. ');
            return null;
        }
        if (!this.checkParam(time)) {
            log.warn('Time Indicates the end time of the task and cannot be empty. ');
            return null;
        }
        if (!this.checkParam(followerIds)) {
            log.warn('FollowerIds is mandatory and cannot be empty. ');
            return null;
        }
        let fUserIds = this.getUserId(followerIds.split(','));
        if (!this.checkParam(fUserIds)) {
            log.warn('fUserIds is mandatory and cannot be empty. ');
            return null;
        }
        return {
            "rich_summary": richSummary,
            "rich_description": richDescription,
            "collaboratorIds": cUserIds,
            "time": time,
            "followerIds": fUserIds,
            "title": title,
            "url": url
        }
    }

    convertEventAndCreateTaskV2(eventData, nodeConfig) {
        if (!this.checkParam(eventData)) {
            log.warn("eventData is empty, can not be handled")
            return null;
        }
        let event = eventData.afterData;
        if (!this.checkParam(event)) {
            log.warn("afterData is empty, can not be handled");
            return null;
        }
        if (!this.checkParam(nodeConfig)) {
            log.warn("nodeConfig is empty, can not be handled");
            return null;
        }
        let collaboratorIds;
        let followerIds;
        let richSummary = event[nodeConfig.richSummary];
        let richDescription = event[nodeConfig.richDescription];
        let time = this.timeProcessor(nodeConfig.cutOffTime);
        let title = event[nodeConfig.taskLinkTitle];
        let url = event[nodeConfig.taskUrl];
        if (nodeConfig.userType === 'automatic') {
            collaboratorIds = this.getUserId(event[nodeConfig.ownerArray].split(','));
            followerIds = this.getUserId(event[nodeConfig.followerArray].split(','));
        } else {
            collaboratorIds = this.getUserId(nodeConfig.ownerArrayManual.split(','));
            followerIds = this.getUserId(nodeConfig.followerArrayManual.split(','));
        }

        if (!this.checkParam(richSummary)) {
            log.warn('RichSummary is the title of the task and cannot be empty. ');
            return null;
        }
        if (!this.checkParam(richDescription)) {
            log.warn('RichDescription is the description in the task. It cannot be empty. ');
            return null;
        }
        if (!this.checkParam(collaboratorIds)) {
            log.warn('The collaboratorIds are in charge of the task and cannot be empty. ');
            return null;
        }
        if (!this.checkParam(time)) {
            log.warn('Time Indicates the end time of the task and cannot be empty. ');
            return null;
        }
        if (!this.checkParam(followerIds)) {
            log.warn('FollowerIds is mandatory and cannot be empty. ');
            return null;
        }

        return {
            "rich_summary": richSummary,
            "rich_description": richDescription,
            "collaboratorIds": collaboratorIds,
            "time": time,
            "followerIds": followerIds,
            "title": this.checkParam(title) ? title : "无",
            "url": this.checkParam(url) ? url : "无",
        }
    }

    timeProcessor(time) {
        let cutOffTime = parseInt(new Date().getTime() / 1000) + 3600 * time;
        return cutOffTime.toString();
    }

    sendHttp(sendData) {
        let writeResult = invoker.httpConfig({"timeout": 20000}).invoke("Create task", sendData)
        if (writeResult.httpCode < 200 || writeResult.httpCode >= 300) {
            throw ("create task failed, http code illegal. " + JSON.stringify(writeResult));
        }
        if (!this.checkParam(writeResult.result.code)) {
            throw ("create task failed, lark code illegal. " + JSON.stringify(writeResult));
        }
        if (writeResult.result.code === 0) {
            return true;
        } else {
            log.warn("create task failed. {}", writeResult.result);
        }
        return false;
    }

    getUserId(receivedUser) {
        let userIds = {};
        for (let index = 0; index < receivedUser.length; index++) {
            let splitUserId = receivedUser[index];
            let userIdFromCache = userMap[splitUserId];
            if (this.checkParam(userIdFromCache)) {
                userIds[splitUserId] = userIdFromCache;
                continue;
            }
            if (this.checkParam(splitUserId)) {
                let receiveIdData = invoker.invoke("Get the user ID by phone number or email", {
                    "userMobiles": splitUserId,
                    "userEmails": splitUserId
                });
                if (!this.checkParam(receiveIdData)) {
                    log.warn("Get user id by phone or email {} failed", splitUserId);
                    continue;
                }
                if (!this.checkParam(receiveIdData.result.data)) {
                    log.warn("Get user id by phone or email {}, data is empty", splitUserId);
                    continue;
                }
                let userList = receiveIdData.result.data.user_list;
                if (!(this.checkParam(userList)) && userList.length > 0) {
                    log.warn("Get user id by phone or email {}, user_list is not array or length == 0", splitUserId);
                }
                let userId = userList[0].user_id;
                if (!this.checkParam(userId)) {
                    // 用户：{{phoneOrEmail}}, 这位用户不在应用的可见范围中，
                    // 请确保应用的此用户在当前版本下可见，您可在应用版本管理与发布中查看最新版本下的可见范围，如有必要请在创建新的版本并将此用户添加到可见范围。

                    if (userList.length > 1)
                        userId = userList[1].user_id;
                }
                if (!this.checkParam(userId)) {
                    log.warn('Get user id by phone or email {} failed, user id can not be found or {} is not visible to your application', splitUserId, splitUserId);
                    return null;
                }
                if (!this.checkParam(userIds[splitUserId])) {
                    userIds[splitUserId] = userId;
                }
                userMap[splitUserId] = userId;
            }
        }
        return "\"" + Object.values(userIds).join("\",\"") + "\"";
    }

    checkParam(param) {
        return 'undefined' !== param && null != param;
    }
}