package io.tapdata.common.exception;

import io.tapdata.exception.*;

public enum PDKExceptionEnum {

    TERMINATE_BY_SERVER("terminateByServer", TapPdkTerminateByServerEx.class),
    USER_PWD_INVALID("userPwdInvalid", TapPdkUserPwdInvalidEx.class),
    OFFSET_INVALID("offsetInvalid", TapPdkOffsetOutOfLogEx.class),
    READ_PRIVILEGES("readPrivileges", TapPdkReadMissingPrivilegesEx.class),
    WRITE_PRIVILEGES("writePrivileges", TapPdkWriteMissingPrivilegesEx.class),
    WRITE_TYPE("writeType", TapPdkWriteTypeEx.class),
    WRITE_LENGTH("writeLength", TapPdkWriteLengthEx.class),
    VIOLATE_UNIQUE("violateUnique", TapPdkViolateUniqueEx.class),
    VIOLATE_NULL("violateNull", TapPdkViolateNullableEx.class),
    CDC_CONFIG_INVALID("cdcConfigInvalid", TapPdkViolateNullableEx.class);

    private final String type;
    private final Class<?> clazz;

    PDKExceptionEnum(String type, Class<?> clazz) {
        this.type = type;
        this.clazz = clazz;
    }

    public String getType() {
        return type;
    }

    public Class<?> getClazz() {
        return clazz;
    }

}
