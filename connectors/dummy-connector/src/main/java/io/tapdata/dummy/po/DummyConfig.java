package io.tapdata.dummy.po;

import io.tapdata.dummy.IDummyConfig;
import io.tapdata.dummy.constants.RecordOperators;
import io.tapdata.dummy.utils.DummyUtils;
import io.tapdata.dummy.utils.TapEventBuilder;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;

import java.util.*;

/**
 * Dummy config
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/11 13:13 Create
 */
public class DummyConfig implements IDummyConfig {
    private final DataMap config;

    public DummyConfig(DataMap config) {
        this.config = config;
    }

    @Override
    public List<TapTable> getSchemas() {
        String tableName = config.getString("table_name");
        tableName = DummyUtils.blankDefault(tableName, null);
        if (null != tableName) {
            List<Map<String, Object>> tableFields = config.getValue("table_fields", null);
            if (null != tableFields && !tableFields.isEmpty()) {
                TapTable table = new TapTable(tableName);
                table.setDefaultPrimaryKeys(new ArrayList<>());

                Map<String, Object> dummyField;
                for (int i = 0; i < tableFields.size(); i++) {
                    dummyField = tableFields.get(i);
                    TapField field = new TapField((String) dummyField.get("name"), (String) dummyField.get("type"));
                    field.setDefaultValue(dummyField.get("def"));
                    if (Optional.ofNullable((Boolean) dummyField.get("pri")).orElse(false)) {
                        table.getDefaultPrimaryKeys().add(field.getName());
                        field.setPos(i);
                        field.setPrimaryKey(true);
                        field.primaryKeyPos(table.getDefaultPrimaryKeys().size());
                    }
                    table.add(field);
                }

                return Collections.singletonList(table);
            }
        }

        return new ArrayList<>();
    }

    @Override
    public Long getBatchTimeouts() {
        String val = config.getString("batch_timeouts");
        val = DummyUtils.blankDefault(val, "3000");
        return Long.parseLong(val);
    }

    @Override
    public Long getInitialTotals() {
        String val = config.getString("initial_totals");
        val = DummyUtils.blankDefault(val, "1");
        return Long.parseLong(val);
    }

    @Override
    public Integer getIncrementalInterval() {
        String val = config.getString("incremental_interval");
        val = DummyUtils.blankDefault(val, "1000");
        return Integer.parseInt(val);
    }

    @Override
    public Integer getIncrementalIntervalTotals() {
        String val = config.getString("incremental_interval_totals");
        val = DummyUtils.blankDefault(val, "0");
        return Integer.parseInt(val);
    }

    @Override
    public Set<RecordOperators> getIncrementalTypes() {
        Object val = config.getObject("incremental_types");
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
        if (operators.isEmpty()) {
            operators.add(RecordOperators.Insert);
        }
        return operators;
    }

    @Override
    public Integer getWriteInterval() {
        String val = config.getString("write_interval");
        val = DummyUtils.blankDefault(val, "1000");
        return Integer.parseInt(val);
    }

    @Override
    public Integer getWriteIntervalTotals() {
        String val = config.getString("write_interval_totals");
        val = DummyUtils.blankDefault(val, "1");
        return Integer.parseInt(val);
    }

    @Override
    public Boolean isWriteLog() {
        Object val = config.getObject("write_log");
        return Boolean.TRUE.equals(val);
    }

    @Override
    public TapEventBuilder getTapEventBuilder() {
        return new TapEventBuilder();
    }
}
