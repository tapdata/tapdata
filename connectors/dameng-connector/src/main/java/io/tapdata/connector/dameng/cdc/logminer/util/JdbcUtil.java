package io.tapdata.connector.dameng.cdc.logminer.util;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class JdbcUtil {

    public static Map<String, Object> buildLogData(ResultSetMetaData metaData,
                                                   ResultSet logContentsRs,
                                                   ZoneId sysTimezone) throws Exception {
        int columnCount = metaData.getColumnCount();
        Map<String, Object> logData = new HashMap<>();
        // put result set to logData
        for (int i = 0; i < columnCount; i++) {
            String fieldName = metaData.getColumnName(i + 1);
            Object object = JdbcUtil.getObject(logContentsRs, i + 1, Integer.MAX_VALUE);
            if (object instanceof Timestamp) {
                object = convertRedoContentTimestamp((Timestamp) object, sysTimezone);
            }
            logData.put(fieldName, object);
        }
        return logData;
    }

    public static Timestamp convertRedoContentTimestamp(Timestamp timestamp, ZoneId sysTimezone) {
        if (timestamp == null || sysTimezone == null) {
            return timestamp;
        }
        // jdbc会获取jvm的默认时区，作为timestamp的时区，需要去除
        LocalDateTime localDateTime = timestamp.toLocalDateTime();

        // 根据oracle的system time zone，将timestamp进行时区转换
        timestamp = Timestamp.from(localDateTime.atZone(sysTimezone).toInstant());

        return timestamp;
    }

    public static Object getObject(ResultSet resultSet, int columnIndex, int lobMaxSize) throws SQLException {
        return getObject(resultSet, null, columnIndex, resultSet.getMetaData().getColumnType(columnIndex), lobMaxSize);
    }

    public static Object getObject(ResultSet resultSet, Map<String, Object> after, int columnIndex, int columnType, int lobMaxSize) throws SQLException {
        return getObject(resultSet, after, columnIndex, columnType, lobMaxSize, null);
    }

    public static Object getObject(ResultSet resultSet, Map<String, Object> after, int columnIndex, int columnType, int lobMaxSize, Calendar calendar) throws SQLException {
        Object object;
        ResultSetMetaData metaData = resultSet.getMetaData();
        String columnTypeName = metaData.getColumnTypeName(columnIndex);
        if ("money".equals(columnTypeName)) {
            if (resultSet.getMetaData().toString().contains("postgresql")) {
                // org.postgresql.jdbc.PgResultSetMetaData@17e9b246
                // PG will call convert to double, but sometime it will not suit, so it throws PSQLException
                String s = resultSet.getString(columnIndex);
                if (null != s) {
                    // PGMoney only support double value, will throw error when exceed
                    // money geString return string like "-$1,124,567.89" and "$1,124,567.89"
                    boolean negative = (s.charAt(0) == '-');
                    // Remove - (for negative) & currency symbol
                    String s1 = s.substring(negative ? 2 : 1);
                    // Strip out any, in currency
                    s1 = s1.replace(",", "");

                    object = new BigDecimal(negative ? "-" + s1 : s1);
                } else {
                    object = null;
                }
            } else {
//        // sqlserver 获取的 money 是 BigDecimal，double 可能装不下
                object = resultSet.getObject(columnIndex);
            }
        } else {
            switch (columnType) {
                case Types.DATE:
                case Types.TIMESTAMP:
                case TIMESTAMP_LTZ_TYPE:
                case TIMESTAMP_TZ_TYPE:
                case Types.TIME_WITH_TIMEZONE:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    try {
                        if (calendar != null) {
                            object = resultSet.getTimestamp(columnIndex, calendar);
                        } else {
                            object = resultSet.getTimestamp(columnIndex);
                        }
                        if (object == null) {
                            object = resultSet.getString(columnIndex);
                        }
                    } catch (SQLException e) {
                        // mysql非法时间特殊处理
                        object = resultSet.getString(columnIndex);
                    }
                    break;
                case Types.TIME:
                    try {
                        object = resultSet.getTime(columnIndex);
                    } catch (SQLException e) {
                        object = resultSet.getString(columnIndex);
                    }
                    break;
                case Types.BIT:
                    try {
                        object = resultSet.getObject(columnIndex);
                    } catch (SQLException e) {
                        // GaussDB driver will call convert to boolean, but sometime it will not suit, so it throws PSQLException
                        // and sometimes bit(n)'s value is bit-string like "10011"
                        object = resultSet.getString(columnIndex);
                    }
                    break;
                default:
                    object = resultSet.getObject(columnIndex);
                    break;
            }
        }

//        if (object instanceof String && Types.CHAR == columnType) {
//            object = ((String) object).trim();
//        }
        return object;
    }

    // https://docs.oracle.com/cd/E16338_01/appdev.112/e13995/constant-values.html#oracle_jdbc_OracleTypes_TIMESTAMPTZ
    protected final static int TIMESTAMP_TZ_TYPE = -101;
    // https://docs.oracle.com/cd/E16338_01/appdev.112/e13995/constant-values.html#oracle_jdbc_OracleTypes_TIMESTAMPLTZ
    protected final static int TIMESTAMP_LTZ_TYPE = -102;
}
