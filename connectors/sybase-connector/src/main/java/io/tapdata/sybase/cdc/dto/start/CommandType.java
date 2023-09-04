package io.tapdata.sybase.cdc.dto.start;

/**
 * @author GavinXiao
 * @description CommandType create by Gavin
 * @create 2023/7/13 17:42
 **/
public enum CommandType {
    FULL("full"),
    CDC("real-time"),
    AUTO("full");
    String type;

    CommandType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static String type(CommandType type) {
        if (null == type) return FULL.type;
        return type.type;
    }
}
