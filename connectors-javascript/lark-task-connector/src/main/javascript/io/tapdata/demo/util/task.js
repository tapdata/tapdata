var invoker = loadAPI();
var userMap = {};

class CreateTask {
    create(connectionConfig, nodeConfig, eventDataList) {
        let succeedDataArr = [];
        for (let index = 0; index < eventDataList.length; index++) {
            let event = eventDataList[index];
            let data;
            try {
                data = this.convertEventAndCreateTask(event);
                if (data == null) {
                    continue;
                }
            } catch (e) {
                log.warn("Failed to create a task: {} please check whether the submitted parameters are correct: {}", e.message, JSON.stringify(event.afterData));
            }
            if (this.sendHttp(data)) {
                succeedDataArr.push(event);
            }
            if (!isAlive()) {
                return succeedDataArr;
            }
        }
        return succeedDataArr;
    }

    convertEventAndCreateTask(eventData) {
        let event = eventData.afterData;
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
            log.warn('RichDescription is the description in the task. It cannot be left empty. ');
            return null;
        }
        if (!this.checkParam(collaboratorIds)) {
            log.warn('The collaboratorIds are in charge of the task and cannot be empty. ');
            return null;
        }
        let cUserIds = this.getUserId(collaboratorIds.split(","));
        if (!this.checkParam(time)) {
            log.warn('Time Indicates the end time of the task and cannot be empty. ');
            return null;
        }
        if (!this.checkParam(followerIds)) {
            log.warn('FollowerIds is mandatory and cannot be empty. ');
            return null;
        }
        let fUserIds = this.getUserId(followerIds.split(","));

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

    sendHttp(sendData) {
        let writeResult;
        writeResult = invoker.invoke("Create task", sendData);
        if ((writeResult.httpCode >= 200 || writeResult.httpCode < 300) && this.checkParam(writeResult.result.code) && writeResult.result.code === 0) {
            return true;
        } else {
            throw ("Failed to send the HTTP request. ");
        }
    }

    getUserId(receivedUser) {
        let userIds = {};
        for (let index = 0; index < receivedUser.length; index++) {
            let spiltUserId = receivedUser[index];
            let userIdFromCache = userMap[spiltUserId];
            if (this.checkParam(userIdFromCache)) {
                userIds[spiltUserId] = userIdFromCache;
                continue;
            }
            if (this.checkParam(spiltUserId)) {
                let receiveIdData = invoker.invoke("Get the user ID by phone number or email", {
                    "userMobiles": spiltUserId,
                    "userEmails": spiltUserId
                });
                let userId = receiveIdData.result.data.user_list[0].user_id;
                if (!this.checkParam(userId)) {
                    // 用户：{{phoneOrEmail}}, 这位用户不在应用的可见范围中，
                    // 请确保应用的此用户在当前版本下可见，您可在应用版本管理与发布中查看最新版本下的可见范围，如有必要请在创建新的版本并将此用户添加到可见范围。
                    userId = receiveIdData.result.data.user_list[1].user_id;
                }
                if (!this.checkParam(userId)) {
                    log.warn(' User: {}, this user is not in the visible range of the application. Please ensure that this user of the application is visible under the current version. You can view the visible range under the latest version in the application version management and release. If necessary, create a new version and add this user to the visible range, message is: {}', event.phone, event.content);
                    continue;
                }
                if (!this.checkParam(userIds[spiltUserId])) {
                    userIds[spiltUserId] = userId;
                }
                userMap[spiltUserId] = userId;
            }
        }
        return "\"" + Object.values(userIds).join("\",\"") + "\"";
    }

    checkParam(param) {
        return 'undefined' !== param && null != param;
    }
}