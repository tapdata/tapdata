package com.tapdata.tm.utils;

import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@Nested
public class CommonStringUtilsTest {
    @Test
    void testEscape(){
        Assertions.assertEquals("ab..c", CommonStringUtils.escape("ab.c", '.'));
        Assertions.assertEquals("ab....c", CommonStringUtils.escape("ab..c", '.'));
    }
}
