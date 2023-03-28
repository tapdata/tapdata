package io.tapdata.coding.service.command;

import io.tapdata.coding.service.loader.ProjectsLoader;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.base.ConnectorBase.entry;
import static io.tapdata.base.ConnectorBase.map;

public class DescribeUserProjects implements Command {
    private static final String TAG = DescribeUserProjects.class.getSimpleName();

    @Override
    public CommandResult commandResult(TapConnectionContext tapConnectionContext, CommandInfo commandInfo, AtomicReference<String> accessToken) {
        Map<String, Object> connectionConfig = commandInfo.getConnectionConfig();
        if (Checker.isEmptyCollection(connectionConfig)) {
            throw new CoreException("Connection Config must be null or empty.");
        }
        tapConnectionContext.setConnectionConfig(DataMap.create(connectionConfig));
        ProjectsLoader loader = ProjectsLoader.create(tapConnectionContext, accessToken);
        List<Map<String, Object>> maps = null;
        try {
            maps = loader.myProjectList();
        }catch (Exception e){
            String msg  = e.getMessage();
            if (Objects.nonNull(msg) && msg.contains("\"Error\":{\"Message\":\"\",\"Code\":\"InvalidParameterValue\"}")){
                throw new CoreException("OAuth authorization information has expired, please re authorize.");
            }else
                throw new CoreException(msg);
        }
        Map<String, Object> pageResult = new HashMap<>();
        if (Checker.isEmptyCollection(maps)) {
            TapLogger.debug(TAG, "Command not get result.");
            return Command.emptyResult();
        }
        Integer page = 1;
        Integer size = maps.size();
        Integer total = maps.size();
        pageResult.put("page", page);
        pageResult.put("size", size);
        pageResult.put("total", total);
        List<Map<String, Object>> resultList = new ArrayList<>();
        maps.forEach(map -> resultList.add(map(entry("label", map.get("DisplayName")), entry("value", map.get("Name")))));
        pageResult.put("items", resultList);
        return new CommandResult().result(pageResult);
    }
}
