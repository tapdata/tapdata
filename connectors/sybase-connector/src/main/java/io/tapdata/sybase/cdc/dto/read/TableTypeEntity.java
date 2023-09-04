package io.tapdata.sybase.cdc.dto.read;

import io.tapdata.entity.error.CoreException;
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
        String typeUppercase = type.toUpperCase();
        switch (typeUppercase) {
            case "DATE":
                this.typeNum = Type.DATE;
                break;
            case "TIME":
                this.typeNum = Type.TIME;
                break;
            case "BIGTIME":
                this.typeNum = Type.BIG_TIME;
                break;
            case "INT":
            case "TINYINT":
                this.typeNum = Type.INT;
                break;
            case "SMALLINT":
                this.typeNum = Type.SMALLINT;
                break;
            case "BIGINT":
                this.typeNum = Type.BIGINT;
                break;
            case "DOUBLE":
                this.typeNum = Type.DOUBLE;
                break;
            case "SMALLMONEY":
            case "MONEY":
            case "REAL":
            case "BIT":
                this.typeNum = Type.BIT;
                break;
            case "IMAGE":
                this.typeNum = Type.IMAGE;
                break;
            default:
                if (type.contains("CHAR")
                        || type.contains("TEXT")
                        || type.contains("SYSNAME")) {
                    this.typeNum = Type.CHAR;
                } else if (type.contains("SMALLDATETIME")) {
                    this.typeNum = Type.DATETIME;
                } else if (type.contains("DATETIME")
                        //|| "SMALLDATETIME".equals(type)
                        //|| "DATETIME".equals(type)
                        //|| "BIGDATETIME".equals(type)
                        || "TIMESTAMP".equals(type)) {
                    this.typeNum = Type.DATETIME;
                } else if (type.startsWith("FLOAT")) {
                    this.typeNum = Type.FLOAT;
                } else if (type.startsWith("DECIMAL")) {
                    this.typeNum = Type.DECIMAL;
                } else if (type.startsWith("NUMERIC")) {
                    this.typeNum = Type.NUMERIC;
                } else if (type.startsWith("VARBINARY")) {
                    this.typeNum = Type.BINARY;
                } else if (type.contains("BINARY")) {
                    this.typeNum = Type.BINARY;
                } else if (type.contains("IMAGE")) {
                    this.typeNum = Type.IMAGE;
                } else {
                    this.typeNum = Type.FAIL;
                }
        }
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
    }
}
