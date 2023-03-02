class CommandStage {
    exec(commandName) {
        switch (commandName) {
            case 'GetAppInfo':
                return new GetAppInfo();
            case 'GetReceiverOfUsers':
                return new GetReceiverOfUsers();
            case 'GetReceiverOfUsers1':
                return new GetReceiverOfUsers();
            default :
                return null;
        }
    }
}

class Command {
    command() {
    }
}

class GetAppInfo extends Command {
    command(connectionConfig, nodeConfig, commandInfo) {
        let app = invoker.invoke("Obtain application information", connectionConfig).result;
        let isApp = 'undefined' !== app && null != app && 'undefined' !== app.data && null != app.data && 'undefined' !== app.data.app;
        return {
            "setValue": {
                "app_name": {
                    "data": isApp ? app.data.app.app_name : ""
                }
            }
        };
    }
}

class GetReceiverOfUsers extends Command {
    userNamePrefix = "用户：";

    command() {
        let result = [];
        let users = this.getUsers();
        result.push(...users);
        return {
            "page": 1,
            "size": 1,
            "total": result.length,
            "items": result
        };
    }

    getUsers() {
        //获取用户列表
        // 1. 获取所有子部门
        let dept = [{"department_id": '0', "name": "根部门"}];
        let hasMore = false;
        let pageTokenDept = '';
        do {
            // https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/contact-v3/department/children
            let departmentsData = invoker.invoke('GetSubDept', {"page_token": pageTokenDept}).result;
            hasMore = departmentsData.data.has_more;
            let deptArr = departmentsData.data.items;
            if ('undefined' !== deptArr && null != deptArr && deptArr.length > 0) {
                dept.push(...deptArr);
                pageTokenDept = departmentsData.data.page_token;
            }
        } while (isAlive() && hasMore)
        let users = [];
        // 2. 根据子部门获取部门员工
        for (let index = 0; index < dept.length; index++) {
            if (!isAlive()) break;
            let deptId = dept[index].department_id;
            let pageToken = "";
            do {
                // https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/contact-v3/users/find_by_department
                let usersData = invoker.invoke('GetDeptUsers', {
                    "departmentId": deptId,
                    "page_token": pageToken
                }).result;
                hasMore = usersData.data.has_more;
                let userDataArr = usersData.data.items;
                if ('undefined' !== userDataArr && null != userDataArr && userDataArr.length > 0) {
                    for (let i = 0; i < userDataArr.length; i++) {
                        if (!isAlive()) break;
                        let u = userDataArr[i];
                        users.push({"value": u.open_id, "label": this.userNamePrefix + u.name});
                    }
                    pageToken = usersData.data.page_token;
                }
            } while (isAlive() && hasMore)
        }
        return users;
    }
}