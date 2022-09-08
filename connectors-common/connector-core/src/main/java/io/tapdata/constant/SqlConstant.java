package io.tapdata.constant;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tapdata on 01/12/2017.
 */
public class SqlConstant {

    public static final String REDO_LOG_OPERATION_COMMIT = "COMMIT";
    public static final String REDO_LOG_OPERATION_ROLLBACK = "ROLLBACK";
    public static final String REDO_LOG_OPERATION_INSERT = "INSERT";
    public static final String REDO_LOG_OPERATION_UPDATE = "UPDATE";
    public static final String REDO_LOG_OPERATION_DELETE = "DELETE";
    public static final String REDO_LOG_OPERATION_DDL = "DDL";

    public static final String REDO_LOG_OPERATION_LOB_TRIM = "LOB_TRIM";
    public static final String REDO_LOG_OPERATION_LOB_WRITE = "LOB_WRITE";
    public static final String REDO_LOG_OPERATION_SEL_LOB_LOCATOR = "SEL_LOB_LOCATOR";
    public static final String REDO_LOG_OPERATION_SELECT_FOR_UPDATE = "SELECT_FOR_UPDATE";

    public static final String REDO_LOG_OPERATION_UNSUPPORTED = "UNSUPPORTED";

    public enum OracleDataTypeConvertEnum {
        DATE("DATE", "DATE"),
        NUMBER("NUMBER", "DOUBLE"),
        FLOAT("FLOAT", "DOUBLE"),
        CHAR("CHAR", "STRING"),
        NCHAR("NCHAR", "STRING"),
        VARCHAR("VARCHAR", "STRING"),
        VARCHAR2("VARCHAR2", "STRING"),
        NVARCHAR2("NVARCHAR2", "STRING"),
        CLOB("CLOB", "STRING"),
        NCLOB("NCLOB", "STRING"),
        LONG("LONG", "STRING"),
        RAW("RAW", "BYTE"),
        BLOB("BLOB", "BYTE"),
        ;

        final String jdbcType;

        final String javaType;

        private static final Map<String, OracleDataTypeConvertEnum> map = new HashMap<>();

        static {
            for (OracleDataTypeConvertEnum dataTypeConvertEnum : OracleDataTypeConvertEnum.values()) {
                map.put(dataTypeConvertEnum.getJdbcType(), dataTypeConvertEnum);
            }
        }

        OracleDataTypeConvertEnum(String jdbcType, String javaType) {
            this.jdbcType = jdbcType;
            this.javaType = javaType;
        }

        public String getJdbcType() {
            return jdbcType;
        }

        public String getJavaType() {
            return javaType;
        }

        public static OracleDataTypeConvertEnum fromJdbcType(String jdbcType) {
            return map.get(jdbcType);
        }
    }

}
