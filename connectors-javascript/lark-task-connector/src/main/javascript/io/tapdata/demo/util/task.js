var invoker = loadAPI();

class createTask {
    create(connectionConfig, nodeConfig, eventDataList) {
        let succeedDataArr = [];
        for (let index = 0; eventDataList.length; index++) {
            let event = eventDataList[index];
            let data = this.convertEventAndCreateTask(event);
            if (null != data) {
                let r = this.sendHttp(data);
                if (this.checkParam(r)) {
                    succeedDataArr.push(event.afterData);
                }
            }
        }
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
            log.error('RichSummary is the title of the task and cannot be empty. ');
        }
        if (!this.checkParam(richDescription)) {
            log.error('RichDescription is the description in the task. It cannot be left empty. ');
        }
        if (!this.checkParam(collaboratorIds)) {
            log.error('The collaboratorIds are in charge of the task and cannot be empty. ');
        }
        if (!this.checkParam(time)) {
            log.error('Time Indicates the end time of the task and cannot be empty. ');
        }
        if (!this.checkParam(richDescription)) {
            log.error('RichDescription is the description in the task. It cannot be left empty. ');
        }

        return {
            "rich_summary": richSummary,
            "rich_description": richDescription,
            "collaborator_ids": collaboratorIds,
            "time": time,
            "follower_ids": followerIds,
            "title": title,
            "url": url
        }
    }

    sendHttp(sendData) {
        let writeResult;
        try {
            writeResult = invoker.invoke("Create task", sendData);
        } catch (e) {
            throw e;
        }
        if (writeResult.result.code === 0) {
            return true;
        } else {
            log.warn(writeResult.result.msg);
        }
    }

    getUserId(receivedUser) {
        let receiveId = receiveOpenIdMap[receivedUser];
        switch (receiveType) {
            case 'phone': {
                if (!this.checkParam(receiveId)) {
                    let receiveIdData = invoker.httpConfig({'timeout': 1000}).invoke("GetOpenIdByPhone", {
                        'userMobiles': receivedUser,
                        "userEmails": receivedUser
                    });
                    receiveId = receiveIdData.result.data.user_list[0].user_id;
                    if (!this.checkParam(receiveId)) {
                        // 用户：{{phoneOrEmail}}, 这位用户不在应用的可见范围中，
                        // 请确保应用的此用户在当前版本下可见，您可在应用版本管理与发布中查看最新版本下的可见范围，如有必要请在创建新的版本并将此用户添加到可见范围。
                        receiveId = receiveIdData.result.data.user_list[1].user_id;
                    }
                    if (!this.checkParam(receiveId)) {
                        log.warn(' User: {}, this user is not in the visible range of the application. Please ensure that this user of the application is visible under the current version. You can view the visible range under the latest version in the application version management and release. If necessary, create a new version and add this user to the visible range, message is: {}', event.phone, event.content);
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

    checkParam(param) {
        return 'undefined' !== param && null != param;
    }
}