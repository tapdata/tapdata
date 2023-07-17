package io.tapdata.sybase.cdc.dto.start;

/**
 * @author GavinXiao
 * @description OverwriteType create by Gavin
 * @create 2023/7/17 18:09
 **/
public enum OverwriteType {
    OVERWRITE("overwrite"),
    RESUME("resume"),
    AUTO("overwrite");
    String type;

    OverwriteType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static String type(OverwriteType type) {
        if (null == type) return AUTO.type;
        return type.type;
    }

    public static OverwriteType type(String type){
        if (null == type) return AUTO;
        OverwriteType[] values = values();
        for (OverwriteType value : values) {
            if (value.type.equals(type)) return value;
        }
        return AUTO;
    }
}
