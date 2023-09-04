package io.tapdata.sybase.cdc.dto.read;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;

/**
 * @author GavinXiao
 * @description TableTypeEntity create by Gavin
 * @create 2023/7/31 15:19
 **/
public class TableTypeEntity {
    String type;
    String name;
    int length;
    int typeNum;


    public TableTypeEntity(String type, String name, int length) {
        if (null == type || "".equals(type.trim())) {
            throw new CoreException("Data type can not be empty, name: {}", name);
        }
        this.type = type;
        this.name = name;
        this.length = length;
        this.typeNum = Type.type(type);
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getTypeNum() {
        return this.typeNum;
    }

    public static class Type {
        public static final int CHAR = 0;
        public static final int TEXT = 1;
        public static final int DATE = 2;
        public static final int TIME = 3;
        public static final int BIG_TIME = 4;
        public static final int DATETIME = 5;
        public static final int SMALL_DATETIME = 51;
        public static final int INT = 6;
        public static final int SMALLINT = 7;
        public static final int BIGINT = 8;
        public static final int DOUBLE = 9;
        public static final int BIT = 10;
        public static final int IMAGE = 11;
        public static final int FLOAT = 12;
        public static final int DECIMAL = 13;
        public static final int NUMERIC = 14;
        public static final int BINARY = 15;
        public static final int FAIL = -1;

        public static int type(String typeName) {
            String type = typeName.toUpperCase();
            switch (type) {
                case "DATE":
                    return Type.DATE;
                case "TIME":
                    return Type.TIME;
                case "BIGTIME":
                    return Type.BIG_TIME;
                case "INT":
                case "TINYINT":
                    return Type.INT;
                case "SMALLINT":
                    return Type.SMALLINT;
                case "BIGINT":
                    return Type.BIGINT;
                case "DOUBLE":
                    return Type.DOUBLE;
                case "SMALLMONEY":
                case "MONEY":
                case "REAL":
                case "BIT":
                    return Type.BIT;
                case "IMAGE":
                    return Type.IMAGE;
                default:
                    if (type.contains("CHAR") || type.contains("SYSNAME")) {
                        return Type.CHAR;
                    } else if (type.contains("TEXT")) {
                        return Type.TEXT;
                    } else if (type.contains("SMALLDATETIME")) {
                        return Type.DATETIME;
                    } else if (type.contains("DATETIME")
                            //|| "SMALLDATETIME".equals(type)
                            //|| "DATETIME".equals(type)
                            //|| "BIGDATETIME".equals(type)
                            || "TIMESTAMP".equals(type)) {
                        return Type.DATETIME;
                    } else if (type.startsWith("FLOAT")) {
                        return Type.FLOAT;
                    } else if (type.startsWith("DECIMAL")) {
                        return Type.DECIMAL;
                    } else if (type.startsWith("NUMERIC")) {
                        return Type.NUMERIC;
                    } else if (type.startsWith("VARBINARY")) {
                        return Type.BINARY;
                    } else if (type.contains("BINARY")) {
                        return Type.BINARY;
                    } else if (type.contains("IMAGE")) {
                        return Type.IMAGE;
                    } else {
                        return Type.FAIL;
                    }
            }
        }
    }
}
