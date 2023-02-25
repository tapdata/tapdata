package io.tapdata.common;

public enum FileTypeEnum {

    EXCEL("excel"),
    CSV("csv"),
    JSON("json"),
    XML("xml"),
    UNSUPPORTED("unsupported");

    private final String name;

    FileTypeEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static FileTypeEnum fromValue(String name) {
        for (FileTypeEnum type : FileTypeEnum.values()) {
            if (type.getName().equals(name)) {
                return type;
            }
        }
        return UNSUPPORTED;
    }
}
