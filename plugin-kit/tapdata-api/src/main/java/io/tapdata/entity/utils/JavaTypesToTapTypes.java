package io.tapdata.entity.utils;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.schema.type.*;
import io.tapdata.entity.schema.value.DateTime;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.*;

public class JavaTypesToTapTypes {
    public static final String JAVA_String = "String";
    public static final String JAVA_Date = "Date";
    public static final String JAVA_Double = "Double";
    public static final String JAVA_Float = "Float";
    public static final String JAVA_BigDecimal = "BigDecimal";
    public static final String JAVA_Long = "Long";
    public static final String JAVA_Map = "Map";
    public static final String JAVA_Array = "Array";
    public static final String JAVA_Binary = "Binary";
    public static final String JAVA_Boolean = "Boolean";
    public static final String JAVA_Integer = "Integer";


    public static TapType toTapType(String javaType) {
        if(javaType == null)
            throw new CoreException(TapAPIErrorCodes.ERROR_MISSING_JAVA_TYPE, "Missing javaType for TapType conversion");

        switch (javaType) {
            case JAVA_Array:
                return tapArray();
            case JAVA_Float:
                return tapNumber().maxValue(BigDecimal.valueOf(Float.MAX_VALUE)).minValue(BigDecimal.valueOf(-Float.MAX_VALUE)).fixed(false).scale(8).precision(38);
            case JAVA_Date:
                return tapDateTime().fraction(3);
            case JAVA_BigDecimal:
                return tapNumber().maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(-Double.MAX_VALUE)).precision(10000).scale(100).fixed(true);
            case JAVA_Double:
                return tapNumber().maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(-Double.MAX_VALUE)).scale(17).precision(309).fixed(false);
            case JAVA_Long:
                return tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE));
            case JAVA_Map:
                return tapMap();
            case JAVA_String:
                return tapString().bytes(1024L);
            case JAVA_Integer:
                return tapNumber().maxValue(BigDecimal.valueOf(Integer.MAX_VALUE)).minValue(BigDecimal.valueOf(Integer.MIN_VALUE));
            case JAVA_Binary:
                return tapBinary();
            case JAVA_Boolean:
                return tapBoolean();
            default:
                throw new CoreException(TapAPIErrorCodes.ERROR_UNKNOWN_JAVA_TYPE, "Unknown javaType {} for TapType conversion", javaType);
        }
    }

    public static void main(String[] args) {
        float a = 12.32232132132132432432423423423432432432423423432432432874324329847932847234f;
        float b = 123213213213211123213213213211123456789.000000000000000000123213123123123123f;
        float c = 0.32232132132132432432423423423432432432423423432432432874324329847932847234f;
        System.out.println("a " + a);
        System.out.println("b " + b);
        System.out.println("c " + c);


        double a1 = 12.32232132132132432432423423423432432432423423432432432874324329847932847234d;
        double b1 = 123213213213211d;
        double c1 = 0.32232132132132432432423423423432432432423423432432432874324329847932847234d;
        System.out.println("a1 " + a1);
        System.out.println("b1 " + b1);
        System.out.println("c1 " + c1);
    }

    public static TapType toTapType(Object theValue) {
        if(theValue instanceof Float) {
            return toTapType(JAVA_Float);
        } else if(theValue instanceof BigDecimal) {
            return toTapType(JAVA_BigDecimal);
        } else if(theValue instanceof Double) {
            return toTapType(JAVA_Double);
        } else if(theValue instanceof Long) {
            return toTapType(JAVA_Long);
        }else if(theValue instanceof Integer) {
            return toTapType(JAVA_Integer);
        } else if(theValue instanceof Number) {
            return toTapType(JAVA_Double);
        } else if(theValue instanceof Date || theValue instanceof Instant || theValue instanceof DateTime) {
            return toTapType(JAVA_Date);
        } else if(theValue instanceof Collection) {
            return toTapType(JAVA_Array);
        } else if(theValue instanceof Map) {
            return toTapType(JAVA_Map);
        } else if(theValue instanceof String || theValue instanceof StringBuilder || theValue instanceof StringBuffer) {
            return toTapType(JAVA_String);
        } else if(theValue instanceof Boolean) {
            return toTapType(JAVA_Boolean);
        } else if(theValue instanceof byte[]) {
            return toTapType(JAVA_Binary);
        }
        return null;
    }

    public static TapType toTapType(Class<? extends TapType> aClass) {
        if (aClass == TapArray.class) {
            return toTapType(JAVA_Array);
        } else if (aClass == TapBinary.class) {
            return toTapType(JAVA_Binary);
        } else if (aClass == TapBoolean.class) {
            return toTapType(JAVA_Boolean);
        } else if (aClass == TapDateTime.class) {
            return toTapType(JAVA_Date);
        } else if (aClass == TapDate.class) {
            return tapDate();
        } else if (aClass == TapTime.class) {
            return tapTime();
        } else if (aClass == TapYear.class) {
            return tapYear();
        } else if (aClass == TapMap.class) {
            return toTapType(JAVA_Map);
        } else if (aClass == TapNumber.class) {
            return toTapType(JAVA_Double);
        } else if (aClass == TapRaw.class) {
            return tapRaw();
        } else if (aClass == TapString.class) {
            return toTapType(JAVA_String);
        } else {
            throw new CoreException(TapAPIErrorCodes.ERROR_UNKNOWN_TAP_TYPE, "Unknown tapType {} for TapType conversion", aClass.getName());
        }
    }
}
