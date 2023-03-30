package io.tapdata.coding.service.command;

import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public interface Command {
    static final String TAG = Command.class.getSimpleName();

    public static Command command(String command) {
        if (Checker.isEmpty(command)) return null;
        Class<?> clz = null;
        try {
            clz = Class.forName("io.tapdata.coding.service.command." + command);
            return ((Command) clz.newInstance());
        } catch (ClassNotFoundException e) {
            TapLogger.debug(TAG, "ClassNotFoundException for Command {}", command);
        } catch (InstantiationException e1) {
            TapLogger.debug(TAG, "InstantiationException for Command {}", command);
        } catch (IllegalAccessException e2) {
            TapLogger.debug(TAG, "IllegalAccessException for Command {}", command);
        }
        return null;
    }

    public static CommandResult command(TapConnectionContext tapConnectionContext, CommandInfo commandInfo, AtomicReference<String> accessToken) {
        TapLogger.debug(TAG, "Command info {}", commandInfo);
        String command = commandInfo.getCommand();
        if (Checker.isEmpty(command)) {
            throw new CoreException("Command can not be NULL or not be empty.");
        }
        if (Checker.isEmpty(command))
            throw new RuntimeException(MessageFormat.format("Command is empty from command info {}", commandInfo));
        Class<?> clz = null;
        try {
            clz = Class.forName("io.tapdata.coding.service.command." + command);
            return ((Command) clz.newInstance()).commandResult(tapConnectionContext, commandInfo, accessToken);
        } catch (Throwable e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public CommandResult commandResult(TapConnectionContext tapConnectionContext, CommandInfo commandInfo, AtomicReference<String> accessToken);

    public static CommandResult emptyResult() {
        Map<String, Object> pageResult = new HashMap<>();
        pageResult.put("page", 0);
        pageResult.put("size", 0);
        pageResult.put("total", 0);
        pageResult.put("items", new ArrayList<>());
        return new CommandResult().result(pageResult);
    }
}
