package com.tapdata.tm.modules.constant;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ParamTypeEnumTest {

    @Test
    void testParamTypeEnum() {
        assertTrue(ParamTypeEnum.isValid("number","1"));
        assertTrue(ParamTypeEnum.isValid("string","1"));
        assertTrue(ParamTypeEnum.isValid("date","2021-01-01"));
        assertTrue(ParamTypeEnum.isValid("datetime","2021-01-01 00:00:00"));
        assertTrue(ParamTypeEnum.isValid("time","00:00:00"));
        assertTrue(ParamTypeEnum.isValid("boolean","true"));
        assertTrue(ParamTypeEnum.isValid("array","[1,2,3]"));
        assertTrue(ParamTypeEnum.isValid("array",null));
        assertTrue(ParamTypeEnum.checkArray(null));
        Assertions.assertThrows(Exception.class, () -> ParamTypeEnum.isValid("array","xxx"))  ;
    }
}