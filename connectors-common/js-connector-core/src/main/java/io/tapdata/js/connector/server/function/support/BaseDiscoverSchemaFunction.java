package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.js.connector.enums.JSTableKeys;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.iengine.ScriptEngineInstance;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.*;
import java.util.function.Consumer;
//Table Structure Statement:
//(1)Only one table name:
/**
 * "TableName";
 * <p>
 * ["Table1","Table2"];
 * <p>
 * ["Table1","Table2"];
 */
//(2)Return table list only:
/**
 * ["Table1","Table2"];
 * */
//(3)Return detailed table information:
// [{},{},['','',''],{}],list of tables , every item as a table info. if each item be list, the list must be table name(String) list.
//The field has the following properties: type  |  default  |  nullable  |  isPrimaryKey  |  autoInc | comment
//default, isPrimaryKey
//String#byte, Number#value

/**
 * [
 *    {
 *        "name":"User",
 *        "fields":{
 *            "key1":{
 *                type:"String",
 *                default:"111",
 *                bytes: 150,
 *            },
 *            "key2":{
 *                type:"Number",
 *                default:10
 *            }
 *        },
 *        "comment":""
 *    },
 *    {
 *        "name":"Pet",
 *        "fields":{
 *            "key1":{
 *                type:"String",
 *                default:"111"
 *            },
 *            "key2":{
 *                type:"Number",
 *                default:10
 *            }
 *        },
 *        "comment":""
 *    },
 *    ['Table3','Table4','Table5']
 * ];
 * */
public class BaseDiscoverSchemaFunction extends FunctionBase {
    private static final String TAG = BaseDiscoverSchemaFunction.class.getSimpleName();

    private BaseDiscoverSchemaFunction() {
        super();
        super.functionName = JSFunctionNames.DISCOVER_SCHEMA;
    }

    public static BaseDiscoverSchemaFunction discover(LoadJavaScripter script) {
        if (Objects.isNull(script)) {
            script = ScriptEngineInstance.instance().script();
        }
        BaseDiscoverSchemaFunction function = new BaseDiscoverSchemaFunction();
        function.javaScripter(script);
        return function;
    }

    public void invoker(TapConnectionContext connectionContext, Consumer<List<TapTable>> consumer) throws Throwable {
        if (!this.javaScripter.functioned(functionName.jsName())) {
            TapLogger.info(TAG, "Not found 'discover_schema' which the implementation of a named function, cannot load and scan tables.");
            return;
        }
        Object invoker = this.javaScripter.invoker(JSFunctionNames.DISCOVER_SCHEMA.jsName(), connectionContext);
        if (Objects.isNull(invoker)) {
            TapLogger.info(TAG, "No table information was loaded after discoverSchema was executed.");
            return;
        }
        List<TapTable> tables = new ArrayList<>();
        Set<Map.Entry<String, Object>> discoverSchema = new HashSet<>();
        if (invoker instanceof Map) {
            discoverSchema = ((Map<String, Object>) invoker).entrySet();
        } else if (invoker instanceof Collection) {
            Collection<Object> tableCollection = (Collection<Object>) invoker;
            tableCollection.stream().filter(Objects::nonNull).forEach(tab -> this.covertTable(tables, tab));
        } else {
            String tableId = String.valueOf(invoker);
            tables.add(new TapTable(tableId, tableId));
        }
        if (!discoverSchema.isEmpty()) {
            discoverSchema.stream().filter(Objects::nonNull).forEach(entry -> this.covertTable(tables, entry.getValue()));
        }
        consumer.accept(tables);
    }

    private void covertTable(List<TapTable> tables, Object value) {
        if (value instanceof String) {
            String table = (String) value;
            TapTable tapTable = new TapTable(table, table);
            tapTable.setNameFieldMap(new LinkedHashMap<>());
            tables.add(tapTable);
        } else if (value instanceof Map) {
            Map<String, Object> tableMap = (Map<String, Object>) value;
            TapTable tapTable = new TapTable();
            Object tableIdObj = tableMap.get(JSTableKeys.TABLE_NAME);
            if (Objects.isNull(tableIdObj)) {
                TapLogger.warn(TAG, "The declared table has no table name. Please assign a value to the 'name' field of the table.");
                return;
            }
            String tableId = (String) tableIdObj;
            Object tableCommentObj = tableMap.get(JSTableKeys.TABLE_COMMENT);

            Object fieldsMapObj = tableMap.get(JSTableKeys.TABLE_FIELD);
            if (Objects.isNull(fieldsMapObj)) {
                TapLogger.warn(TAG, String.format("The declared table does not contain any field information. If necessary, please add field information to Table [%s].", tableId));
            } else {
                Map<String, Object> columnMap = (Map<String, Object>) fieldsMapObj;
                columnMap.entrySet().stream().filter(field ->
                        Objects.nonNull(field) && Objects.nonNull(field.getKey())
                ).forEach(column -> tapTable.add(this.field(column)));
            }
            tapTable.setId(tableId);
            tapTable.setName(tableId);
            tapTable.setComment(Objects.isNull(tableCommentObj) ? null : String.valueOf(tableCommentObj));
            tables.add(tapTable);
        } else if (value instanceof Collection) {
            Collection<Object> collection = (Collection<Object>) value;
            collection.stream().filter(Objects::nonNull).forEach(table -> {
                String tableName = String.valueOf(table);
                tables.add(new TapTable(tableName, tableName));
            });
        }
    }

    private TapField field(Map.Entry<String, Object> column) {
        TapField field = new TapField();
        String fieldName = column.getKey();
        field.setName(fieldName);
        Object fieldInfoObj = column.getValue();
        if (Objects.nonNull(fieldInfoObj)) {
            Map<String, Object> fieldInfo = (Map<String, Object>) fieldInfoObj;
            Object fieldTypeObj = fieldInfo.get(JSTableKeys.TABLE_FIELD_TYPE);
            Object fieldDefaultObj = fieldInfo.get(JSTableKeys.TABLE_FIELD_DEFAULT_VALUE);
            Object fieldNullAbleObj = fieldInfo.get(JSTableKeys.TABLE_FIELD_NULLABLE);
            Object fieldPrimaryKeyObj = fieldInfo.get(JSTableKeys.TABLE_FIELD_PRIMARY_KEY);
            Object fieldAutoIncObj = fieldInfo.get(JSTableKeys.TABLE_FIELD_AUTO_INC);
            Object fieldFieldCommentObj = fieldInfo.get(JSTableKeys.TABLE_FIELD_COMMENT);
            field.setComment(Objects.isNull(fieldFieldCommentObj) ? null : String.valueOf(fieldFieldCommentObj));
            try {
                field.setAutoInc((Boolean) fieldAutoIncObj);
            } catch (Exception ignored) {
            }
            field.setDataType(Objects.isNull(fieldTypeObj) ? null : String.valueOf(fieldTypeObj));
            field.setDefaultValue(Objects.isNull(fieldDefaultObj) ? null : String.valueOf(fieldDefaultObj));
            try {
                field.setNullable((Boolean) fieldNullAbleObj);
            } catch (Exception ignored) {
            }
            try {
                field.setPrimaryKey((Boolean) fieldPrimaryKeyObj);
            } catch (Exception ignored) {
            }
        }
        return field;
    }

}
