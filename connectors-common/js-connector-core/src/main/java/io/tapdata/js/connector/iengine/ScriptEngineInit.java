package io.tapdata.js.connector.iengine;

import io.tapdata.pdk.apis.javascript.core.ConnectorLog;

import javax.script.ScriptEngine;
import java.util.Objects;

public class ScriptEngineInit {
    public static ScriptEngineInit create(){
        return new ScriptEngineInit();
    }
    public void init(ScriptEngine scriptEngine){
        if (Objects.isNull(scriptEngine)) return;
        scriptEngine.put("log", new ConnectorLog());
    }
}
