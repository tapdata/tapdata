package com.tapdata.tm.base.field;

public interface CollectionField {

    String field();

    @SafeVarargs
    static <T extends CollectionField> String[] fields(T... field) {
        String[] names = new String[field.length];
        for (int i = 0; i < field.length; i++) {
            names[i] = field[i].field();
        }
        return names;
    }
}
