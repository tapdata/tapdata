package io.tapdata.http.util;

import io.tapdata.entity.error.CoreException;
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

    public static ScriptEvel create(ScriptEngine scriptEngine, TapConnectionContext connectionContext) {
        ScriptEvel scriptEvel = new ScriptEvel();
        scriptEvel.scriptEngine = scriptEngine;
        scriptEvel.connectionContext = connectionContext;
        return scriptEvel;
    }

    public void evalSourceForEngine(ScriptEngine scriptEngine) {
        scriptEngine.put("log", new TapConnectorLog(connectionContext.getLog()));
        StringBuilder buildInMethod = new StringBuilder();
        buildInMethod.append("var DateUtil = Java.type(\"io.tapdata.http.util.engine.DateUtil\");\n");
        buildInMethod.append("var UUIDGenerator = Java.type(\"io.tapdata.http.util.engine.UUIDGenerator\");\n");
        buildInMethod.append("var idGen = Java.type(\"io.tapdata.http.util.engine.UUIDGenerator\");\n");
        buildInMethod.append("var HashMap = Java.type(\"java.util.HashMap\");\n");
        buildInMethod.append("var LinkedHashMap = Java.type(\"java.util.LinkedHashMap\");\n");
        buildInMethod.append("var ArrayList = Java.type(\"java.util.ArrayList\");\n");
        buildInMethod.append("var uuid = UUIDGenerator.uuid;\n");
        buildInMethod.append("var JSONUtil = Java.type('io.tapdata.http.util.engine.JSONUtil');\n");
        buildInMethod.append("var HanLPUtil = Java.type(\"io.tapdata.http.util.engine.HanLPUtil\");\n");
        buildInMethod.append("var split_chinese = HanLPUtil.hanLPParticiple;\n");
        buildInMethod.append("var util = Java.type(\"io.tapdata.http.util.engine.Util\");\n");
        buildInMethod.append("var MD5Util = Java.type(\"io.tapdata.http.util.engine.MD5Util\");\n");
        buildInMethod.append("var MD5 = function(str){return MD5Util.crypt(str, true);};\n");
        buildInMethod.append("var Collections = Java.type(\"java.util.Collections\");\n");
        buildInMethod.append("var sleep = function(ms){\n" +
                "var Thread = Java.type(\"java.lang.Thread\");\n" +
                "Thread.sleep(ms);\n" +
                "}\n");
        try {
            scriptEngine.eval(buildInMethod.toString());
        } catch (Exception e) {
            throw new CoreException("Can not eval js utils, msg: {}", e.getMessage());
        }
    }

    public void evalSourceForSelf() {
        evalSourceForEngine(this.scriptEngine);
    }
}
