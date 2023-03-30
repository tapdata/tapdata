package io.tapdata.coding.service.command;

import io.tapdata.coding.service.loader.CodingStarter;
import io.tapdata.coding.service.loader.IterationsLoader;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.base.ConnectorBase.entry;
import static io.tapdata.base.ConnectorBase.map;

public class DescribeCodingProjects implements Command {

    @Override
    public CommandResult commandResult(TapConnectionContext tapConnectionContext, CommandInfo commandInfo, AtomicReference<String> accessToken) {
        String command = commandInfo.getCommand();
        Map<String, Object> argMap = commandInfo.getArgMap();
        String token = null;
        String teamName = null;
        String projectName = null;

        Map<String, Object> connectionConfig = commandInfo.getConnectionConfig();
        if (Checker.isEmpty(connectionConfig)) {
            throw new IllegalArgumentException("ConnectionConfig cannot be null");
        }

        Object tokenObj = connectionConfig.get("token");
        Object teamNameObj = connectionConfig.get("teamName");
        if (Checker.isNotEmpty(tokenObj)) {
            token = (tokenObj instanceof String) ? (String) tokenObj : String.valueOf(tokenObj);
        }
        if (Checker.isNotEmpty(teamNameObj)) {
            teamName = (teamNameObj instanceof String) ? (String) teamNameObj : String.valueOf(teamNameObj);
        }

        Object projectNameObj = connectionConfig.get("projectName");
        if (Checker.isNotEmpty(projectNameObj)) {
            projectName = (projectNameObj instanceof String) ? (String) projectNameObj : String.valueOf(projectNameObj);
        }
        if ("DescribeIterationList".equals(command) && Checker.isEmpty(projectName)) {
            throw new CoreException("ProjectName must be not Empty or not null.");
        }
        if (Checker.isEmpty(token)) {
            TapLogger.warn(TAG, "token must be not null or not empty.");
            throw new CoreException("token must be not null or not empty.");
        }
        if (Checker.isEmpty(teamName)) {
            TapLogger.warn(TAG, "teamName must be not null or not empty.");
            throw new CoreException("teamName must be not null or not empty.");
        }

        String upToken = token.toUpperCase();
        token = (upToken.startsWith("TOKEN ") ? token : "token " + token);
        HttpEntity<String, String> header = HttpEntity.create().builder("Authorization", token);
        HttpEntity<String, Object> body = IterationsLoader.create(tapConnectionContext, accessToken, argMap)
                .commandSetter(command, HttpEntity.create());
        if ("DescribeIterationList".equals(command) && Checker.isNotEmpty(projectName)) {
            body.builder("ProjectName", projectName);
        }

        String url = String.format(CodingStarter.OPEN_API_URL, teamName);
        CodingHttp http = CodingHttp.create(header.getEntity(), body.getEntity(), url);
        Map<String, Object> postResult = http.post();

//        Object response = postResult.get("Response");
//        Map<String,Object> responseMap = (Map<String, Object>) response;
//        if (Checker.isEmpty(response)){
//            //TapLogger.info(TAG, "HTTP request exception, list acquisition failed: {} ", CodingStarter.OPEN_API_URL+"?Action="+command);
//            throw new RuntimeException("Get list failed: " + url +"?Action="+command);
//        }

        Map<String, Object> pageResult = new HashMap<>();
        Object dataObj = postResult.get("Data");
        if (Checker.isEmpty(dataObj)) {
            return Command.emptyResult();
        }
        Map<?, ?> data = (Map<?, ?>) dataObj;
        if ("DescribeIterationList".equals(command)) {
            Object listObj = data.get("List");
            List<Map<String, Object>> searchList = new ArrayList<>();
            if (Checker.isNotEmpty(listObj)) {
                searchList = (List<Map<String, Object>>) listObj;
            }
            Integer page = Checker.isEmpty(data.get("Page")) ? 0 : Integer.parseInt(data.get("Page").toString());
            Integer size = Checker.isEmpty(data.get("PageSize")) ? 0 : Integer.parseInt(data.get("PageSize").toString());
            Integer total = Checker.isEmpty(data.get("TotalPage")) ? 0 : Integer.parseInt(data.get("TotalPage").toString());
            Integer rows = Checker.isEmpty(data.get("TotalRow")) ? 0 : Integer.parseInt(data.get("TotalRow").toString());
            pageResult.put("page", page);
            pageResult.put("size", size);
            pageResult.put("total", total);
            pageResult.put("rows", rows);
            List<Map<String, Object>> resultList = new ArrayList<>();
            searchList.forEach(map -> {
                resultList.add(map(entry("label", map.get("Name")), entry("value", map.get("Code"))));
            });
            pageResult.put("items", resultList);
        } else if ("DescribeCodingProjects".equals(command)) {
            Object listObj = data.get("ProjectList");
            List<Map<String, Object>> searchList = new ArrayList<>();
            if (Checker.isNotEmpty(listObj)) {
                searchList = (List<Map<String, Object>>) listObj;
            }
            Integer page = Checker.isEmpty(data.get("PageNumber")) ? 0 : Integer.parseInt(data.get("PageNumber").toString());
            Integer size = Checker.isEmpty(data.get("PageSize")) ? 0 : Integer.parseInt(data.get("PageSize").toString());
            Integer total = Checker.isEmpty(data.get("TotalCount")) ? 0 : Integer.parseInt(data.get("TotalCount").toString());
            pageResult.put("page", page);
            pageResult.put("size", size);
            pageResult.put("total", total);
            List<Map<String, Object>> resultList = new ArrayList<>();
            searchList.forEach(map -> {
                resultList.add(map(entry("label", map.get("DisplayName")), entry("value", map.get("Name"))));
            });
            pageResult.put("items", resultList);
        } else {
            throw new CoreException("Command only support [DescribeIterationList] or [DescribeCodingProjects].");
        }
        return new CommandResult().result(pageResult);
    }
}
