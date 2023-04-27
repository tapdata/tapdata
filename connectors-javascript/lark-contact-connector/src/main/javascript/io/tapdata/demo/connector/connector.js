function discoverSchema(connectionConfig) {
    let app = invoker.invoke("Obtain application information").result;
    let appName = app.data.app.app_name;
    return [
        {
        'name': 'users',
        'fields': {
            "country": {
              "type": "string"
            },
            "work_station": {
              "type": "string"
            },
            "gender": {
              "type": "integer"
            },
            "city": {
              "type": "string"
            },
            "open_id": {
              "type": "string"
            },
            "description": {
              "type": "string"
            },
            "employee_no": {
              "type": "string"
            },
            "join_time": {
              "type": "DateTime"
            },
            "nickname": {
              "type": "string"
            },
            "union_id": {
              "type": "string"
            },
            "en_name": {
              "type": "string"
            },
            "job_title": {
              "type": "string"
            },
            "email": {
              "type": "string"
            },
            "mobile": {
              "type": "string"
            },
            "avatar": {
              "type": "object",
              "properties": {
                "avatar_640": {
                  "type": "string"
                },
                "avatar_origin": {
                  "type": "string"
                },
                "avatar_72": {
                  "type": "string"
                },
                "avatar_240": {
                  "type": "string"
                }
              }
            },
            "department_ids": {
              "type": "array",
              "items": {
                "type": "string"
              }
            },
            "enterprise_email": {
              "type": "string"
            },
            "employee_type": {
              "type": "integer"
            },
            "user_id": {
              "type": "string",
              'isPrimaryKey': true,
              'primaryKeyPos': 1
            },
            "name": {
              "type": "string"
            },
            "orders": {
              "type": "array",
              "items": {
                "type": "object",
                "properties": {
                  "is_primary_dept": {
                    "type": "boolean"
                  },
                  "user_order": {
                    "type": "integer"
                  },
                  "department_id": {
                    "type": "string"
                  },
                  "department_order": {
                    "type": "integer"
                  }
                }
              }
            },
            "is_tenant_manager": {
            "type": "boolean"
             },
              "leader_user_id": {
              "type": "string"
              },
               "mobile_visible": {
               "type": "boolean"
               },
            "status": {
              "type": "object",
              "properties": {
                "is_activated": {
                  "type": "boolean"
                },
                "is_frozen": {
                  "type": "boolean"
                },
                "is_exited": {
                  "type": "boolean"
                },
                "is_resigned": {
                  "type": "boolean"
                }
        }
    }
}}]}

function connectionTest(connectionConfig) {
    let app = invoker.invoke("Obtain application information").result;
    log.warn('app info'+ JSON.stringify(app))
    let isApp = 'undefined' !== app && null != app && 'undefined' !== app.data && null != app.data && 'undefined' !== app.data.app;
    let testItem = [{
        "test": "Get App info",
        "code": isApp ? 1 : -1,
        "result": isApp ? "App name is: " + app.data.app.app_name : "Can not get App info, please check you App ID and App Secret."
    }];
    return testItem;
}

const tableMapping = {
    'users': 'GetDeptUsers',
}
function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
log.warn("Start Batch Read ------------------------------")
    if(!offset || !offset.tableName){
            offset = {
                tableName:tableName
            };
        }
          let tokenResult = invoker.invokeWithoutIntercept("Obtain the App Token and Tenant Token");
          if (tokenResult.result.code === 0){
            connectionConfig.Authorization = tokenResult.result.tenant_access_token;
          }
          subDepartmentResult = invoker.invokeWithoutIntercept("GetSubDept", {Authorization: "Bearer " + connectionConfig.Authorization, page_token: ''});
          if(subDepartmentResult.result.data && subDepartmentResult.result.data.items.length>0){
          for (let i = 0; i < subDepartmentResult.result.data.items.length; i++) {
            const departmentId = subDepartmentResult.result.data.items[i].open_department_id;
            iterateAllData(tableMapping[tableName], offset, {departmentId : departmentId}, (result, offsetNext, error) => {
            if(result && result.data && result.data.items && result.data.items.length>0){
            if(result && result.data && result.data.page_token){
            offset['page_token'] = 'page_token=' + result.data.page_token
            }
            }
             batchReadSender.send(result.data.items,tableName,offset);
             if(!result || !result.data || !result.data.page_token){
             return false
             }
             else{
             return false
             }
             return isAlive()
             });
          }
          }
          batchReadSender.send(offset);
}

function updateToken(connectionConfig, nodeConfig, apiResponse) {
    if (apiResponse.result.code !== 99991663 && apiResponse.result.code !== 99991661) return null;
    let result = invoker.invokeWithoutIntercept("Obtain the App Token and Tenant Token");
    if (result.result.code === 0){
        connectionConfig.Authorization = result.result.tenant_access_token;
        return {"Authorization": "Bearer " + result.result.tenant_access_token};
    }
    else log.error('Cannot get tenant access token, please check your app_id or app_secret or check api named GetAppToken. {}', result.result);
}

function commandCallback(connectionConfig, nodeConfig, commandInfo) {
    let commandName = commandInfo.command;
    let exec = new CommandStage().exec(commandInfo.command);
    if (null != exec) return exec.command(connectionConfig, nodeConfig, commandInfo);
}
