package io.tapdata.http.command;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;

/**
 * @author GavinXiao
 * @description HistoryEvent create by Gavin
 * @create 2023/5/24 16:10
 **/
public class HistoryEvent implements Command {
    @Override
    public CommandResult execCommand(TapConnectionContext tapConnectionContext, CommandInfo commandInfo) {

        return Command.emptyResult();
    }
}
