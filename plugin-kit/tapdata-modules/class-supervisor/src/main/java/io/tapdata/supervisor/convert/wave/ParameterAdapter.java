package io.tapdata.supervisor.convert.wave;

import io.tapdata.supervisor.utils.JavassistTag;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public enum ParameterAdapter {
    OBJECT("L${};", "*"),
    OBJECT_ARR("[L${};", "*\\[\\]"),
    VOID("V", "void"),
    BOOL("Z", "boolean"),
    INT("I", "int"),
    INT_ARR("[I", "int[]"),
    CHAR("C", "char"),
    CHAR_ARR("[C", "char[]"),
    BYTE("B", "byte"),
    BYTE_ARR("[B", "byte[]"),
    LONG("L", "long"),
    Long_ARR("[L", "long[]"),
    DOUBLE("D", "double"),
    DOUBLE_ARR("[D", "double[]"),
    SHORT("S", "short"),
    SHORT_ARR("[S", "short[]"),
    FLOAT("F", "float"),
    FLOAT_ARR("[F", "float[]"),
    ;
    String ct;
    String java;

    ParameterAdapter(String ct, String java) {
        this.ct = ct;
        this.java = java;
    }

    public static String adapt(List<String> args, String returnType) {
        StringBuilder builder = new StringBuilder("(");
        if (Objects.nonNull(args) && !args.isEmpty()) {
            for (String arg : args) {
                String tag = tag(arg);
                if (null != tag)
                    builder.append(tag);
            }
        }
        builder.append(")").append(Optional.of(tag(returnType)).orElse(VOID.ct));
        return builder.toString();
    }

    public static String tag(String name) {
        if (null == name || name.trim().equals(JavassistTag.EMPTY)) return null;
        ParameterAdapter[] values = values();
        for (ParameterAdapter value : values) {
            if (name.trim().equals(value.java)) return value.ct;
        }
        if (name.trim().matches(OBJECT_ARR.java)) return OBJECT_ARR.ct;
        return OBJECT.ct;
    }
}
