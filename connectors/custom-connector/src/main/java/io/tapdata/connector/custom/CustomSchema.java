package io.tapdata.connector.custom;

import io.tapdata.connector.custom.config.CustomConfig;
import io.tapdata.connector.custom.core.LoadSchemaCore;
import io.tapdata.connector.custom.util.CustomLog;
import io.tapdata.connector.custom.util.ScriptUtil;
import io.tapdata.constant.SyncTypeEnum;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.kit.EmptyKit;
import org.apache.commons.lang3.StringUtils;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomSchema {

    private static final String TAG = CustomSchema.class.getSimpleName();
    private static final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "engine"); //script factory
    private final CustomConfig customConfig;
    private final static int LOAD_SCHEMA_RETRY_TIME = 60;

    public CustomSchema(CustomConfig customConfig) {
        this.customConfig = customConfig;
    }

    public TapTable loadSchema() throws ScriptException {
        String uniqueKeys = customConfig.getUniqueKeys();
        String collectionName = customConfig.getCollectionName();
        if (EmptyKit.isBlank(collectionName)) {
            return null;
        }
        collectionName = collectionName.trim();
        TapTable tapTable = TapSimplify.table(collectionName);
        String script;
        switch (SyncTypeEnum.fromValue(customConfig.getSyncType())) {
            case INITIAL_SYNC:
                script = customConfig.getHistoryScript();
                break;
            case CDC:
                script = customConfig.getCdcScript();
                break;
            default:
                script = EmptyKit.isNotBlank(customConfig.getHistoryScript()) ? customConfig.getHistoryScript() : customConfig.getCdcScript();
                break;
        }
        String[] uniqueKeysSplit = EmptyKit.isNotBlank(uniqueKeys) ? uniqueKeys.split(",") : null;
        Map<String, Integer> uniqueKeysMap = new HashMap<>();
        if (uniqueKeysSplit != null && uniqueKeysSplit.length > 0) {
            for (int i = 0; i < uniqueKeysSplit.length; i++) {
                uniqueKeysMap.put(uniqueKeysSplit[i].trim(), i + 1);
            }
        }

        if (StringUtils.isNotBlank(script)) {
            LoadSchemaCore core = new LoadSchemaCore();
            assert scriptFactory != null;
            ScriptEngine scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName(customConfig.getJsEngineName()));
            scriptEngine.eval(ScriptUtil.appendSourceFunctionScript(script, false));
            scriptEngine.put("core", core);
//            scriptEngine.put("log", new CustomLog());
            Thread t = new Thread(ScriptUtil.createScriptRunnable(scriptEngine, ScriptUtil.SOURCE_FUNCTION_NAME));
            TapLogger.info(TAG, "Running script, try to get data and build schema. \n {}", script);
            t.start();
            int time = 0;
            while (time < LOAD_SCHEMA_RETRY_TIME) {
                TapSimplify.sleep(1000);
                if (EmptyKit.isNotEmpty(core.getData())) {
                    Map<String, Object> data = core.getData();
                    data.forEach((k, v) -> {
                        TapField field = new TapField();
                        field.setName(k.trim());
                        if (v == null) {
                            field.dataType("String");
                        } else {
                            //long String => Text
                            if (v instanceof String && ((String) v).length() > 200) {
                                field.dataType("Text");
                            } else if (v instanceof Map) {
                                field.dataType("Map");
                            } else if (v instanceof List) {
                                field.dataType("List");
                            } else {
                                field.dataType(v.getClass().getSimpleName());
                            }
                        }
                        if (uniqueKeysMap.containsKey(k.trim())) {
                            field.setPrimaryKey(true);
                            field.setPrimaryKeyPos(uniqueKeysMap.get(k.trim()));
                            uniqueKeysMap.remove(k.trim());
                        }
                        tapTable.add(field);
                    });
                    break;
                }
                time++;
            }
            if (t.isAlive()) {
                TapLogger.info(TAG, "Running script timeout(10 seconds), stop javascript engine, cannot load any schema from data");
                t.stop();
            }
            if (EmptyKit.isEmpty(tapTable.getNameFieldMap())) {
                TapLogger.info(TAG, "Cannot load schema from script data, Please check that the script is correct。");
            }
        }
        return tapTable;
    }
}
