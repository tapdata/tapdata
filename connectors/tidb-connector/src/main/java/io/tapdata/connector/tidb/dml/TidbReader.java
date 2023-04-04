package io.tapdata.connector.tidb.dml;

import io.tapdata.connector.mysql.MysqlMaker;
import io.tapdata.connector.mysql.SqlMaker;
import io.tapdata.connector.tidb.TidbJdbcContext;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;

import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class TidbReader {

    private final TidbJdbcContext tidbJdbcContext;

    public TidbReader(TidbJdbcContext tidbJdbcContext) {
        this.tidbJdbcContext = tidbJdbcContext;
    }

    public void readWithFilter(TapConnectorContext tapConnectorContext, TapTable tapTable, TapAdvanceFilter tapAdvanceFilter,
                               Predicate<?> stop, Consumer<Map<String, Object>> consumer) throws Throwable {
        SqlMaker sqlMaker = new MysqlMaker();
        String sql = sqlMaker.selectSql(tapConnectorContext, tapTable, tapAdvanceFilter);
        AtomicLong row = new AtomicLong(0L);
        try {
            Set<String> dateTypeSet = dateFields(tapTable);
            tidbJdbcContext.query(sql, rs -> {
                ResultSetMetaData metaData = rs.getMetaData();
                while (rs.next()) {
                    if (null != stop && stop.test(null)) {
                        break;
                    }
                    row.incrementAndGet();
                    Map<String, Object> data = new HashMap<>();
                    for (int i = 0; i < metaData.getColumnCount(); i++) {
                        String columnName = metaData.getColumnName(i + 1);
                        try {
                            Object value;
                            // 抹除 time 字段的时区，兼容 "-838:59:59", "838:59:59" 格式数据
                            if ("TIME".equalsIgnoreCase(metaData.getColumnTypeName(i + 1))) {
                                value = rs.getString(i + 1);
                            } else {
                                value = rs.getObject(i + 1);
                                if (null == value && dateTypeSet.contains(columnName)) {
                                    value = rs.getString(i + 1);
                                }
                            }
                            data.put(columnName, value);
                        } catch (Exception e) {
                            throw new RuntimeException("Read column value failed, row: " + row.get() + ", column name: " + columnName + ", data: " + data + "; Error: " + e.getMessage(), e);
                        }
                    }
                    consumer.accept(data);
                }
            });
        } catch (Throwable e) {
            if (null != stop && stop.test(null)) {
                // ignored error
            } else {
                throw e;
            }
        }
    }

    private Set<String> dateFields(TapTable tapTable) {
        Set<String> dateTypeSet = new HashSet<>();
        tapTable.getNameFieldMap().forEach((n, v) -> {
            switch (v.getTapType().getType()) {
                case TapType.TYPE_DATE:
                case TapType.TYPE_DATETIME:
                    dateTypeSet.add(n);
                    break;
                default:
                    break;
            }
        });
        return dateTypeSet;
    }
}
