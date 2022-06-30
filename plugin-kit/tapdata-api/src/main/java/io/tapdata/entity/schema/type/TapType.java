package io.tapdata.entity.schema.type;

import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.schema.value.TapValue;

import java.io.Serializable;

public abstract class TapType implements Serializable {
    public static final byte TYPE_DATETIME = 1;
    public static final byte TYPE_ARRAY = 2;
    public static final byte TYPE_BOOLEAN = 3;
    public static final byte TYPE_MAP = 4;
    public static final byte TYPE_YEAR = 5;
    public static final byte TYPE_TIME = 6;
    public static final byte TYPE_RAW = 7;
    public static final byte TYPE_NUMBER = 8;
    public static final byte TYPE_BINARY = 9;
    public static final byte TYPE_STRING = 10;
    public static final byte TYPE_DATE = 11;
    protected byte type;

    public static Class<? extends TapType> getTapTypeClass(byte type) {
        switch (type) {
            case TYPE_ARRAY:
                return TapArray.class;
            case TYPE_BINARY:
                return TapBinary.class;
            case TYPE_BOOLEAN:
                return TapBoolean.class;
            case TYPE_DATE:
                return TapDate.class;
            case TYPE_DATETIME:
                return TapDateTime.class;
            case TYPE_MAP:
                return TapMap.class;
            case TYPE_NUMBER:
                return TapNumber.class;
            case TYPE_RAW:
                return TapRaw.class;
            case TYPE_STRING:
                return TapString.class;
            case TYPE_TIME:
                return TapTime.class;
            case TYPE_YEAR:
                return TapYear.class;
        }
        return null;
    }

    public abstract TapType cloneTapType();
    public abstract Class<? extends TapValue<?, ?>> tapValueClass();
    public abstract ToTapValueCodec<?> toTapValueCodec();

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }
}
