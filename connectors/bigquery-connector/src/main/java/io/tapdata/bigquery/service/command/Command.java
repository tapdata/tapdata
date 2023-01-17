package io.tapdata.bigquery.service.command;

import io.tapdata.bigquery.util.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public interface Command {
    public static final String TAG = Command.class.getSimpleName();

    public static Command command(String command){
        if (Checker.isEmpty(command)) return null;
        Class clz = null;
        try {
            clz = Class.forName("io.tapdata.bigquery.service.command."+command);
            return ((Command)clz.newInstance());
        } catch (ClassNotFoundException e) {
            TapLogger.debug(TAG, "ClassNotFoundException for Command {}",command);
        } catch (InstantiationException e1) {
            TapLogger.debug(TAG, "InstantiationException for Command {}",command);
        } catch (IllegalAccessException e2) {
            TapLogger.debug(TAG, "IllegalAccessException for Command {}",command);
        }
        return null;
    }

    public static CommandResult command(TapConnectionContext tapConnectionContext, CommandInfo commandInfo){
        String command = commandInfo.getCommand();
        if(Checker.isEmpty(command)){
            throw new CoreException("Command cannot be empty.");
        }
        if (Checker.isEmpty(command)) return null;
        Class clz = null;
        try {
            clz = Class.forName("io.tapdata.bigquery.service.command."+command);
            return ((Command)clz.newInstance()).commandResult(tapConnectionContext, commandInfo);
        } catch (ClassNotFoundException e) {
            TapLogger.debug(TAG, "ClassNotFoundException for Command {}",command);
        } catch (InstantiationException e1) {
            TapLogger.debug(TAG, "InstantiationException for Command {}",command);
        } catch (IllegalAccessException e2) {
            TapLogger.debug(TAG, "IllegalAccessException for Command {}",command);
        }
        return null;
    }

    public CommandResult commandResult(TapConnectionContext tapConnectionContext, CommandInfo commandInfo);

    public static CommandResult emptyResult(){
        Map<String, Object> pageResult = new HashMap<>();
        pageResult.put("page",0);
        pageResult.put("size",0);
        pageResult.put("total",0);
        pageResult.put("items",new ArrayList<>());
        return new CommandResult().result(pageResult);
    }
}
