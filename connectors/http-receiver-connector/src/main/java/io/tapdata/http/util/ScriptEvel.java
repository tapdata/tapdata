package io.tapdata.http.util;

import io.tapdata.entity.logger.TapLog;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import javax.script.ScriptEngine;

/**
 * @author GavinXiao
 * @description ScriptEvelUtil create by Gavin
 * @create 2023/5/24 16:42
 **/
public class ScriptEvel {
    ScriptEngine scriptEngine;
    TapConnectionContext connectionContext;
    public static ScriptEvel create(ScriptEngine scriptEngine, TapConnectionContext connectionContext){
        ScriptEvel scriptEvel = new ScriptEvel();
        scriptEvel.scriptEngine = scriptEngine;
        scriptEvel.connectionContext = connectionContext;
        return scriptEvel;
    }

    public void evalSourceForEngine(ScriptEngine scriptEngine){
        scriptEngine.put("log", new TapConnectorLog(connectionContext.getLog()));
    }

    public void evalSourceForSelf(){
        evalSourceForEngine(this.scriptEngine);
    }
}
