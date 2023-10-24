package io.tapdata.supervisor.convert.wave;

public enum Type {
    PUBLIC("public", 0x0001),
    PRIVATE("public", 0x0002),
    PROTECTED("public", 0x0004),
    STATIC("public", 0x0008),
    FINAL("public", 0x0010),
    SYNCHRONIZED("public", 0x0020),
    VOLATILE("public", 0x0040),
    BRIDGE("public", 0x0040),
    TRANSIENT("public", 0x0080),
    VARARGS("public", 0x0080),
    NATIVE("public", 0x0100),
    INTERFACE("public", 0x0200),
    ABSTRACT("public", 0x0400),
    STRICT("public", 0x0800),
    SYNTHETIC("public", 0x1000),
    ANNOTATION("public", 0x2000),
    ENUM("public", 0x4000),
    MANDATED("public", 0x8000),
    SUPER("public", 0x0020),
    MODULE("public", 0x8000),

    ;
    String tag;
    int value;

    Type(String tag, int value) {
        this.tag = tag;
        this.value = value;
    }

    public static int valueIs(String tag) {
        if (null == tag || "".equals(tag.trim())) return PUBLIC.value;
        Type[] values = values();
        for (Type value : values) {
            if (value.tag.equals(tag.trim())) return value.value;
        }
        return PUBLIC.value;
    }
}
