package io.tapdata.common.ddl.type;

import io.tapdata.common.ddl.wrapper.DDLWrapper;

/**
 * @author samuel
 * @Description
 * @create 2022-07-01 17:17
 **/
public class DDLType {
    private final Type type;
    private final String pattern;
    private final boolean caseSensitive;
    private final String desc;
    private final Class<? extends DDLWrapper<?>>[] ddlWrappers;

    @SafeVarargs
    public DDLType(Type type, String pattern, boolean caseSensitive, String desc, Class<? extends DDLWrapper<?>>... ddlWrappers) {
        this.type = type;
        this.pattern = pattern;
        this.caseSensitive = caseSensitive;
        this.desc = desc;
        this.ddlWrappers = ddlWrappers;
    }

    public Type getType() {
        return type;
    }

    public String getPattern() {
        return pattern;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public String getDesc() {
        return desc;
    }

    public Class<? extends DDLWrapper<?>>[] getDdlWrappers() {
        return ddlWrappers;
    }

    public enum Type {
        ADD_COLUMN,
        CHANGE_COLUMN,
        MODIFY_COLUMN,
        ALTER_COLUMN,
        RENAME_COLUMN,
        DROP_COLUMN,
    }
}
