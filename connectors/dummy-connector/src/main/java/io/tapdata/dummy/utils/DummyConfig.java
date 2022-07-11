package io.tapdata.dummy.utils;

import io.tapdata.dummy.constants.DummyMode;
import io.tapdata.dummy.constants.RecordOperators;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.*;

/**
 * Dummy 配置
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/6/22 11:03 Create
 */
public interface DummyConfig {

    /**
     * 获取连接配置
     *
     * @param connectionContext 连接上下文
     * @return 连接配置
     */
    static DataMap connectionConfig(TapConnectionContext connectionContext) {
        DataMap config = connectionContext.getConnectionConfig();
        if (null == config) {
            throw new IllegalArgumentException(String.format("connection %s config is null", connectionContext.getSpecification().getName()));
        }
        return config;
    }

    /**
     * 模式
     *
     * @param connectionConfig 连接配置
     * @return 模式
     */
    static DummyMode getMode(DataMap connectionConfig) {
        String val = connectionConfig.getValue("mode", null);
        if (DummyMode.Heartbeat.name().equals(val)) {
            return DummyMode.Heartbeat;
        } else {
            return DummyMode.Common;
        }
    }

    /**
     * 获取配置中的模型
     *
     * @param connectionConfig 连接配置
     * @return 模型列表
     */
    static List<TapTable> getSchemas(DataMap connectionConfig) {
        String tableName = connectionConfig.getString("table_name");
        if (null == DummyUtils.blankDefault(tableName, null)) {
            throw new IllegalArgumentException(String.format("connection config property 'table_name' is blank: %s", tableName));
        }

        List<Map<String, Object>> tableFields = connectionConfig.getValue("table_fields", null);
        if (null == tableFields || tableFields.isEmpty()) {
            throw new IllegalArgumentException("connection config property 'table_fields' is empty");
        }

        TapTable table = new TapTable(tableName);
        table.setDefaultPrimaryKeys(new ArrayList<>());

        Map<String, Object> dummyField;
        for (int i = 0; i < tableFields.size(); i++) {
            dummyField = tableFields.get(i);
            TapField field = new TapField((String) dummyField.get("name"), (String) dummyField.get("type"));
            field.setDefaultValue(dummyField.get("def"));
            if (Optional.ofNullable((Boolean) dummyField.get("pri")).orElse(false)) {
                field.primaryKeyPos(i);
                table.getDefaultPrimaryKeys().add(field.getName());
            }
            table.add(field);
        }

//        if (DummyMode.Heartbeat == getMode(connectionConfig)) {
//            TapTable table = new TapTable();
//
//        } else { // Common
//            String schemasStr = connectionConfig.getString("schemas");
//
//        }

        return Collections.singletonList(table);
    }

    static String getConnHeartbeat(DataMap connectionConfig) {
        String val = connectionConfig.getValue("conn_heartbeat", null);
        return DummyUtils.blankDefault(val, null);
    }

    static Long getInitialTotals(DataMap connectionConfig) {
        String val = connectionConfig.getString("initial_totals");
        val = DummyUtils.blankDefault(val, "1");
        return Long.parseLong(val);
    }

    static Integer getIncrementalInterval(DataMap connectionConfig) {
        String val = connectionConfig.getString("incremental_interval");
        val = DummyUtils.blankDefault(val, "1000");
        return Integer.parseInt(val);
    }

    static Integer getIncrementalIntervalTotals(DataMap connectionConfig) {
        String val = connectionConfig.getString("incremental_interval_totals");
        val = DummyUtils.blankDefault(val, "1");
        return Integer.parseInt(val);
    }

    static Set<RecordOperators> getIncrementalTypes(DataMap connectionConfig) {
        Object val = connectionConfig.getObject("incremental_types");
        Set<RecordOperators> operators = new HashSet<>();
        if (val instanceof List) {
            ((List<Integer>) val).forEach(s -> {
                switch (s) {
                    case 1:
                        operators.add(RecordOperators.Insert);
                        break;
                    case 2:
                        operators.add(RecordOperators.Update);
                        break;
                    case 3:
                        operators.add(RecordOperators.Delete);
                        break;
                    default:
                        break;
                }
            });
        }
        return operators;
    }

    static Integer getWriteInterval(DataMap connectionConfig) {
        String val = connectionConfig.getString("write_interval");
        val = DummyUtils.blankDefault(val, "1000");
        return Integer.parseInt(val);
    }

    static Integer getWriteIntervalTotals(DataMap connectionConfig) {
        String val = connectionConfig.getString("write_interval_totals");
        val = DummyUtils.blankDefault(val, "1");
        return Integer.parseInt(val);
    }

    static Boolean isWriteLog(DataMap connectionConfig) {
        Object val = connectionConfig.getObject("write_log");
        return Boolean.TRUE.equals(val);
    }
}
