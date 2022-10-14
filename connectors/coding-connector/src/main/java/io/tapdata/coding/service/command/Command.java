package io.tapdata.coding.service.command;

import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;

public interface Command {
    static final String TAG = Command.class.getSimpleName();
    public static Command command(String command){
        if (Checker.isEmpty(command)) return null;
        Class clz = null;
        try {
            clz = Class.forName("io.tapdata.coding.service.command."+command);
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
            throw new CoreException("Command can not be NULL or not be empty.");
        }
        if (Checker.isEmpty(command)) return null;
        Class clz = null;
        try {
            clz = Class.forName("io.tapdata.coding.service.command."+command);
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
}
