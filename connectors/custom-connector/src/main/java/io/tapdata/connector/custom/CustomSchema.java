package io.tapdata.connector.custom;

import io.tapdata.connector.custom.bean.ClientMongoOperator;
import io.tapdata.connector.custom.bean.JavaScriptFunctions;
import io.tapdata.connector.custom.config.CustomConfig;
import io.tapdata.connector.custom.exception.StopException;
import io.tapdata.connector.custom.util.ScriptUtil;
import io.tapdata.constant.SyncTypeEnum;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;
import org.apache.commons.lang3.StringUtils;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomSchema {

    private static final String TAG = CustomSchema.class.getSimpleName();
    private CustomConfig customConfig;
    private static ClientMongoOperator clientMongoOperator;
    private final static int LOAD_SCHEMA_RETRY_TIME = 60;

    public CustomSchema(CustomConfig customConfig) {
        this.customConfig = customConfig;
    }

    public TapTable loadSchema() throws ScriptException {
        String uniqueKeys = customConfig.getUniqueKeys();
        String collectionName = customConfig.getCollectionName();
        collectionName = StringUtils.isBlank(collectionName) ? "newCollections" : collectionName.trim();
        TapTable tapTable = TapSimplify.table(collectionName);
        List<TapField> fields = new ArrayList<>();
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

//        if (StringUtils.isNotBlank(script)) {
//            LoadSchemaCore core = new LoadSchemaCore(null, null, conn, logger, true, null);
//            List<JavaScriptFunctions> javaScriptFunctions = JobUtil.getJavaScriptFunctions(clientMongoOperator);
//            String buildInScript = ScriptUtil.initBuildInMethod(javaScriptFunctions, clientMongoOperator);
//
//            ScriptEngine scriptEngine = ScriptUtil.initScriptEngine(customConfig.getJsEngineName());
//            scriptEngine.eval(buildInScript + ScriptUtil.appendSourceFunctionScript(script, false));
//            scriptEngine.put("core", core);
//            scriptEngine.put("log", logger);
//
//            Thread t = new Thread(createScriptRunnable(scriptEngine));
//
//            TapLogger.info(TAG, "Running script, try to get data and build schema. \n {}", script);
//            t.start();
//
//            int time = 0;
//            while (time < LOAD_SCHEMA_RETRY_TIME) {
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    // do nothing
//                }
//
//                if (MapUtils.isNotEmpty(core.getData())) {
//                    Map<String, Object> data = core.getData();
//
//                    String finalCollectionName = collectionName;
//                    data.forEach((k, v) -> {
//                        RelateDatabaseField relateDatabaseField = new RelateDatabaseField();
//
//                        relateDatabaseField.setField_name(k.trim());
//                        if (v == null) {
//                            relateDatabaseField.setData_type("String");
//                        } else {
//                            relateDatabaseField.setData_type(v.getClass().getSimpleName());
//                        }
//                        relateDatabaseField.setTable_name(finalCollectionName);
//                        if (uniqueKeysMap.containsKey(k.trim())) {
//                            relateDatabaseField.setPrimary_key_position(uniqueKeysMap.get(k.trim()));
//                            relateDatabaseField.setKey(ConnectorConstant.SCHEMA_PRIMARY_KEY);
//                            uniqueKeysMap.remove(k.trim());
//                        }
//
//                        fields.add(relateDatabaseField);
//                    });
//
//                    break;
//                }
//
//                time++;
//            }
//
//            if (t.isAlive()) {
//                TapLogger.info(TAG, "Running script timeout(10 seconds), stop javascript engine, cannot load any schema from data");
//                t.stop();
//            }
//
//            if (EmptyKit.isEmpty(fields)) {
//                TapLogger.info(TAG, "Cannot load schema from script data, Please check that the script is correct。");
//            }
//        }
//
//        getFieldsByUnikeys(fields, uniqueKeysMap, collectionName);
//
//        relateDataBaseTable.setFields(fields);
//        relateDataBaseTable.setType("table");
//        relateDataBaseTables.add(relateDataBaseTable);

        return tapTable;
    }

    private Runnable createScriptRunnable(ScriptEngine scriptEngine) {
        if (scriptEngine != null) {
            return () -> {
                Invocable invocable = (Invocable) scriptEngine;
                try {
                    invocable.invokeFunction(ScriptUtil.SOURCE_FUNCTION_NAME, new HashMap<>());
                } catch (StopException e) {
                    TapLogger.info(TAG, "Get data and stop script.");
                } catch (ScriptException | NoSuchMethodException | RuntimeException e) {
                    TapLogger.error(TAG, "Run script error when load schema, message: {}", e.getMessage(), e);
                }
            };
        } else {
            return null;
        }
    }
}
