package io.tapdata.zoho.service.commandMode;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.zoho.utils.Checker;

public interface CommandMode {
    public CommandResult command(TapConnectionContext connectionContext,CommandInfo commandInfo);
    /**
     * Strategy to realize Command,Different names implement different methods .please in sub class Override the Function :
     *      public CommandResult command(TapConnectionContext connectionContext,CommandInfo commandInfo);
     * And use this function[getInstanceByName] in your invoker addr.
     * */
    public static CommandResult getInstanceByName(TapConnectionContext connectionContext,CommandInfo commandInfo){
        String command = commandInfo.getCommand();
        if (Checker.isEmpty(command)) return null;
        Class clz = null;
        try {
            clz = Class.forName("io.tapdata.zoho.service.commandMode.impl."+command);
            return ((CommandMode)clz.newInstance()).command(connectionContext,commandInfo);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e1) {
            e1.printStackTrace();
        } catch (IllegalAccessException e2) {
            e2.printStackTrace();
        }
        return null;
    }
}
