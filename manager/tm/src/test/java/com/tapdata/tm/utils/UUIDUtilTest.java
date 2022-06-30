package com.tapdata.tm.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UUIDUtilTest {

    @Test
    void getUUID() {
        System.out.println( UUIDUtil.getUUID());
    }

    @Test
    void get64UUID() {
        System.out.println( UUIDUtil.get64UUID());
    }
}