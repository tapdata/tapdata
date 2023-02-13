package io.tapdata.connector.tidb;

import com.google.common.collect.Lists;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.tapdata.base.ConnectorBase.table;
import static io.tapdata.entity.simplify.TapSimplify.index;
import static io.tapdata.entity.simplify.TapSimplify.indexField;

/**
 * @author Dexter
 */
public class TidbSchemaLoader {
    private final TidbJdbcRunner jdbcRunner;

    public TidbSchemaLoader(TidbJdbcRunner jdbcRunner) {
        this.jdbcRunner = jdbcRunner;
    }

    public List<TapIndex> discoverIndex(String tableName) {
        List<DataMap> indexList = jdbcRunner.queryAllIndexes(Collections.singletonList(tableName));
        List<TapIndex> tapIndexList = TapSimplify.list();
        Map<String, List<DataMap>> indexMap = indexList.stream()
                .collect(Collectors.groupingBy(idx -> idx.getString("index_name"), LinkedHashMap::new, Collectors.toList()));
        indexMap.forEach((key, value) -> tapIndexList.add(makeTapIndex(key, value)));
        return tapIndexList;
    }

    private TapIndex makeTapIndex(String key, List<DataMap> value) {
        TapIndex index = index(key);
        value.forEach(v -> index.indexField(indexField(v.getString("column_name")).fieldAsc("A".equals(v.getString("asc_or_desc")))));
        index.setUnique(value.stream().anyMatch(v -> (boolean) v.get("is_unique")));
        index.setPrimary(value.stream().anyMatch(v -> (boolean) v.get("is_primary")));
        return index;
    }

    public void loadSchema(List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {
        // get all the tables under the schema
        List<DataMap> tableList = jdbcRunner.queryAllTables(tables);
        //paginate by tableSize
        List<List<DataMap>> tableLists = Lists.partition(tableList, tableSize);
        tableLists.forEach(subList -> {
            List<TapTable> tapTableList = TapSimplify.list();
            List<String> subTableNames = subList.stream().map(v -> v.getString("name")).collect(Collectors.toList());
            List<DataMap> columnList = jdbcRunner.queryAllColumns(subTableNames);
            List<DataMap> indexList = jdbcRunner.queryAllIndexes(subTableNames);
            //make up tapTable
            subList.forEach(subTable -> {
                //1、table name/comment
                String table = subTable.getString("name");
                TapTable tapTable = table(table);
                tapTable.setComment(subTable.getString("value"));

                //2、primary key and table index (find primary key from index info)
                List<String> primaryKey = TapSimplify.list();
                List<TapIndex> tapIndexList = TapSimplify.list();
                Map<String, List<DataMap>> indexMap = indexList.stream().filter(idx -> table.equals(idx.getString("table_name")))
                        .collect(Collectors.groupingBy(idx -> idx.getString("index_name"), LinkedHashMap::new, Collectors.toList()));
                indexMap.forEach((key, value) -> {
                    if (value.stream().anyMatch(v -> (boolean) v.get("is_primary"))) {
                        primaryKey.addAll(value.stream().map(v -> v.getString("column_name")).collect(Collectors.toList()));
                    }
                    tapIndexList.add(makeTapIndex(key, value));
                });

                //3、table columns info
                AtomicInteger keyPos = new AtomicInteger(0);
                columnList.stream().filter(col -> table.equals(col.getString("table_name")))
                        .forEach(col -> {
                            TapField tapField = parseField(col); //make up fields
                            tapField.setPos(keyPos.incrementAndGet());
                            tapField.setPrimaryKey(primaryKey.contains(tapField.getName()));
                            tapField.setPrimaryKeyPos(primaryKey.indexOf(tapField.getName()) + 1);
                            tapTable.add(tapField);
                        });
                tapTable.setIndexList(tapIndexList);
                tapTableList.add(tapTable);
            });
            consumer.accept(tapTableList);
        });
    }

    private TapField parseField(DataMap dataMap) {
        return new TapField()
                .name(dataMap.getString("column_name"))
                .dataType(getDataType(dataMap))
                .nullable(dataMap.getValue("is_nullable", true))
                .defaultValue(null)
                .comment(dataMap.getString("comment"));
    }

    private String getDataType(DataMap dataMap) {
        int max_length, fraction, precision, scale;
        String rawType = dataMap.getString("type");
        switch (rawType) {
            case "char":
            case "varchar":
            case "binary":
            case "varbinary":
                max_length = Integer.parseInt(dataMap.getString("max_length"));
                if (max_length == -1) {
                    return rawType + "(max)";
                } else {
                    return rawType + "(" + max_length + ")";
                }
            case "nchar":
            case "nvarchar":
                max_length = Integer.parseInt(dataMap.getString("max_length"));
                if (max_length == -1) {
                    return rawType + "(max)";
                } else {
                    return rawType + "(" + max_length / 2 + ")";
                }
            case "time":
            case "datetime2":
            case "datetimeoffset":
                fraction = Integer.parseInt(dataMap.getString("scale"));
                return rawType + "(" + fraction + ")";
            case "float":
                precision = Integer.parseInt(dataMap.getString("precision"));
                return rawType + "(" + precision + ")";
            case "decimal":
            case "numeric":
                precision = Integer.parseInt(dataMap.getString("precision"));
                scale = Integer.parseInt(dataMap.getString("scale"));
                return rawType + "(" + precision + "," + scale + ")";
            default:
        }

        return rawType;
    }
}
