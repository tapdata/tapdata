package io.tapdata.http.command;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;

import java.util.Collections;
import java.util.Optional;

/**
 * @author GavinXiao
 * @description Command create by Gavin
 * @create 2023/5/24 16:00
 **/
public interface Command {

    static CommandResult command(TapConnectionContext tapConnectionContext, CommandInfo commandInfo){
        if (null == commandInfo)
            return emptyResult();
        String command = Optional.ofNullable(commandInfo.getCommand()).orElse("NOT_FOUND_COMMAND");
        Command com = null;
        switch (command){
            case "TRYRUN":
            case "tryrun":
            case "tryRun":
            case "try_run":
            case "TRY_RUN":
            case "TryRun": com = new TryRun(); break;
            case "History_Event":
            case "HISTORYEVENT":
            case "historyevent":
            case "history_event":
            case "HISTORY_EVENT":
            case "HistoryEvent": com = new HistoryEvent(); break;
        }
        return null == com ? emptyResult() : com.execCommand(tapConnectionContext, commandInfo);
    }

    static CommandResult emptyResult(){
        CommandResult result = new CommandResult().result(Collections.emptyMap());
        result.setData(null);
        return result;
    }

    CommandResult execCommand(TapConnectionContext tapConnectionContext, CommandInfo commandInfo);
}
