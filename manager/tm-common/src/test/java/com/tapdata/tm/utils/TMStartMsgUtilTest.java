package com.tapdata.tm.utils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

class TMStartMsgUtilTest {

    @Test
    void testWriteTMStartMsg() {
        final TmStartMsg tmStartMsg = new TmStartMsg("status", "msg");
        TMStartMsgUtil.writeTMStartMsg(tmStartMsg);
        Assertions.assertTrue(Files.exists(Paths.get(".tmStartMsg.json")));
    }

    @AfterAll
    static void afterAllTests() throws IOException {
        Files.delete(Paths.get(".tmStartMsg.json"));
    }
}
