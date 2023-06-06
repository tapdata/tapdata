package io.tapdata.http.util;

import javax.script.ScriptEngine;

/**
 * @author GavinXiao
 * @description ScriptEvelUtil create by Gavin
 * @create 2023/5/24 16:42
 **/
public class ScriptEvel {
    ScriptEngine scriptEngine;

    public static ScriptEvel create(ScriptEngine scriptEngine){
        ScriptEvel scriptEvel = new ScriptEvel();
        scriptEvel.scriptEngine = scriptEngine;
        return scriptEvel;
    }

    public void evalSourceForEngine(ScriptEngine scriptEngine){

    }

    public void evalSourceForSelf(){
        evalSourceForEngine(this.scriptEngine);
    }
}
