package io.tapdata.entity.utils;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.schema.type.TapType;

import java.math.BigDecimal;

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

    public static TapType toTapType(String javaType) {
        if(javaType == null)
            throw new CoreException(TapAPIErrorCodes.ERROR_MISSING_JAVA_TYPE, "Missing javaType for TapType conversion");

        switch (javaType) {
            case JAVA_Array:
                return tapArray();
            case JAVA_Float:
                return tapNumber().maxValue(BigDecimal.valueOf(Float.MAX_VALUE)).minValue(BigDecimal.valueOf(Float.MIN_VALUE)).fixed(false).scale(8).precision(38);
            case JAVA_Date:
                return tapDateTime().fraction(3);
            case JAVA_BigDecimal:
                return tapNumber().precision(10000).scale(100).fixed(true);
            case JAVA_Double:
                return tapNumber().maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(Double.MIN_VALUE)).scale(17).precision(309).fixed(false);
            case JAVA_Long:
                return tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE));
            case JAVA_Map:
                return tapMap();
            case JAVA_String:
                return tapString();
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
}
