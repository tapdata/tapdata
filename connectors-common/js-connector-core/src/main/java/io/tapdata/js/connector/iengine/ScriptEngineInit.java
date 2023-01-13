package io.tapdata.js.connector.iengine;

import io.tapdata.common.support.core.ConnectorLog;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.Objects;

public class ScriptEngineInit {
    public static ScriptEngineInit create() {
        return new ScriptEngineInit();
    }

    public void init(ScriptEngine scriptEngine) {
        if (Objects.isNull(scriptEngine)) return;
        scriptEngine.put("log", new ConnectorLog());
    }

    public void pushFunctionTool(ScriptEngine scriptEngine) throws ScriptException {
        String script = "function strToVar(str) {\n" +
                "   var json = (new Function(\"return \" + str))();\n" +
                "   return json;\n" +
                "}\n" +
                "function functionGet(){\n" +
                "   hs=[];\n" +
                "   Array.from(top.Object.keys(document.defaultView)).map(\n" +
                "     function (x){\n" +
                "         hs.push(x);\n" +
                "         var sjhs = strToVar(x.toString());\n" +
                "     }\n" +
                "   );\n" +
                "   return hs;\n" +
                "}\n";
        scriptEngine.eval(script);
    }
}
