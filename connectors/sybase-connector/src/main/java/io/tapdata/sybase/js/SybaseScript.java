package io.tapdata.sybase.js;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import java.io.Closeable;
import java.util.*;

import static io.tapdata.base.ConnectorBase.*;

public class SybaseScript {
    private static final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");

    private ScriptEngine scriptEngine;
    private Log log;
    private String script;

    public SybaseScript(String script, Log log) {
        this.log = log;
        if (null == script || "".equals(script.trim())) {
            this.script = "function process (record, tableName) { return record;}";
        } else {
            this.script = script;
        }
        init();
    }

    private SybaseScript init() {
        this.scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT,
                new ScriptOptions().engineName("graal.js"));
        try {
            //buildInMethod.append("var DateUtil = Java.type(\"com.tapdata.constant.DateUtil\");\n");
            //buildInMethod.append("var UUIDGenerator = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
            //buildInMethod.append("var idGen = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
            String buildInMethod = "var HashMap = Java.type(\"java.util.HashMap\");\n" +
                    "var LinkedHashMap = Java.type(\"java.util.LinkedHashMap\");\n" +
                    "var ArrayList = Java.type(\"java.util.ArrayList\");\n" +
                    //buildInMethod.append("var uuid = UUIDGenerator.uuid;\n");
                    //buildInMethod.append("var JSONUtil = Java.type('com.tapdata.constant.JSONUtil');\n");
                    //buildInMethod.append("var HanLPUtil = Java.type(\"com.tapdata.constant.HanLPUtil\");\n");
                    //buildInMethod.append("var split_chinese = HanLPUtil.hanLPParticiple;\n");
                    //buildInMethod.append("var util = Java.type(\"com.tapdata.processor.util.Util\");\n");
                    //buildInMethod.append("var MD5Util = Java.type(\"com.tapdata.constant.MD5Util\");\n");
                    //buildInMethod.append("var MD5 = function(str){return MD5Util.crypt(str, true);};\n");
                    "var Collections = Java.type(\"java.util.Collections\");\n" +
                    //buildInMethod.append("var MapUtils = Java.type(\"com.tapdata.constant.MapUtil\");\n");
                    "var sleep = function(ms){\n" +
                    "var Thread = Java.type(\"java.lang.Thread\");\n" +
                    "Thread.sleep(ms);\n" +
                    "}\n";
            this.scriptEngine.eval(buildInMethod);
            this.scriptEngine.put("log", log);
            this.scriptEngine.eval("function process(record, tableName){" + script + "}");
        } catch (Exception e) {
            throw new CoreException("Unable load sybase script engine, msg: {}", e.getMessage());
        }
        return this;
    }

    public Object invokerProcess(Object... params) {
        return invoker("process", params);
    }

    public Object invoker(String functionName, Object... params) {
        if (Objects.isNull(functionName)) return null;
        if (Objects.isNull(this.scriptEngine)) return null;
        try {
            Invocable invocable = (Invocable) this.scriptEngine;
            Object apply = invocable.invokeFunction(functionName, params);
            return covertData(apply);
        } catch (Exception e) {
            throw new CoreException(String.format("JavaScript Method execution failed, method name -[%s], params are -[%s], message: %s, %s", functionName, toJson(params), e.getMessage(), InstanceFactory.instance(TapUtils.class).getStackTrace(e)));
        }
    }

    public void close() {
        try {
            if (scriptEngine instanceof Closeable) {
                ((Closeable)scriptEngine).close();
            }
        } catch (Exception e) {
            log.warn("Sybase script engine can nor safe to close, msg: {}", e.getMessage());
        }
    }

    public static Object covertData(Object apply) {
        if (Objects.isNull(apply)) {
            return null;
        } else if (apply instanceof Map) {
            return InstanceFactory.instance(TapUtils.class).cloneMap((Map<String, Object>) apply);//fromJson(toJson(apply));
        } else if (apply instanceof Collection) {
            try {
                return new ArrayList<>((List<Object>) apply);//ConnectorBase.fromJsonArray(toJson(apply));
            } catch (Exception e) {
                String toString = apply.toString();
                if (toString.matches("\\(([0-9]+)\\)\\[.*]")) {
                    toString = toString.replaceFirst("\\(([0-9]+)\\)", "");
                }
                return ConnectorBase.fromJsonArray(toString);
            }
        } else{
            return apply;
        }
    }

    public static void eventUtil(SybaseScript script, List<TapEvent>[] tapEvents, Map<String, Object> record, String tableId, long referenceTime) {
        if (null == script) {
            tapEvents[0].add(insertRecordEvent(record, tableId).referenceTime(referenceTime));
        } else {
            Object scriptObj = script.invokerProcess(record, tableId);
            if (scriptObj instanceof Map) {
                tapEvents[0].add(insertRecordEvent((Map<String, Object>) scriptObj, tableId).referenceTime(referenceTime));
            } else if (scriptObj instanceof Collection) {
                Collection<Object> list = (Collection<Object>) scriptObj;
                if (!list.isEmpty()) {
                    list.stream().filter(o -> Objects.nonNull(o) && o instanceof Map).forEach(o -> {
                        tapEvents[0].add(insertRecordEvent((Map<String, Object>) o, tableId).referenceTime(referenceTime));
                    });
                }
            }
        }
    }
    public static void eventUtil(SybaseScript script, List<TapEvent>[] tapEvents, TapRecordEvent record, String tableId) {
        if (null == script) {
            tapEvents[0].add(record);
        } else {
            long time = record.getReferenceTime();
           if (record instanceof TapInsertRecordEvent) {
               eventUtil(script, tapEvents, ((TapInsertRecordEvent) record).getAfter(), tableId, time);
           } else if (record instanceof TapDeleteRecordEvent) {
               Object scriptObj = script.invokerProcess(((TapDeleteRecordEvent) record).getBefore(), tableId);
               if (scriptObj instanceof Map) {
                   tapEvents[0].add(deleteDMLEvent((Map<String, Object>) scriptObj, tableId).referenceTime(time));
               } else if (scriptObj instanceof Collection) {
                   Collection<Object> list = (Collection<Object>) scriptObj;
                   if (!list.isEmpty()) {
                       list.stream().filter(o -> Objects.nonNull(o) && o instanceof Map).forEach(o -> {
                           tapEvents[0].add(deleteDMLEvent((Map<String, Object>) o, tableId).referenceTime(time));
                       });
                   }
               }
           } else if (record instanceof TapUpdateRecordEvent) {
               Object scriptObjAfter = script.invokerProcess(((TapUpdateRecordEvent) record).getAfter(), tableId);
               Object scriptObjBefore = script.invokerProcess(((TapUpdateRecordEvent) record).getBefore(), tableId);
               if (scriptObjAfter instanceof Map && scriptObjBefore instanceof Map) {
                   tapEvents[0].add(updateDMLEvent((Map<String, Object>) scriptObjBefore, (Map<String, Object>) scriptObjAfter, tableId).referenceTime(time));
               } else {
                   throw new CoreException("Sybase script can not handel update record when process script and return result which is not Map");
               }
           }
        }
    }
}
