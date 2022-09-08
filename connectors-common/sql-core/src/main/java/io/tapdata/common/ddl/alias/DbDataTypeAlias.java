package io.tapdata.common.ddl.alias;

import io.tapdata.kit.EmptyKit;

public class DbDataTypeAlias {

    private final String alias;

    public DbDataTypeAlias(String alias) {
        this.alias = alias;
    }

    public String toDataType() {
        if (EmptyKit.isBlank(alias)) {
            return "";
        }
        switch (alias.trim().toLowerCase()) {
            case "integer":
            case "int":
                return toInteger();
            case "char":
            case "character":
                return toChar();
            case "varchar":
            case "character varying":
            case "varchar2":
                return toVarchar();
            case "decimal":
            case "number":
            case "numeric":
            case "dec":
            case "num":
                return toDecimal();
            case "float":
            case "real":
                return toFloat();
            default:
                return alias.trim();
        }
    }

    protected String toInteger() {
        return "int";
    }

    protected String toChar() {
        return "char";
    }

    protected String toVarchar() {
        return "varchar";
    }

    protected String toDecimal() {
        return "decimal";
    }

    protected String toFloat() {
        return "float";
    }
}
