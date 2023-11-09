package com.tapdata.tm.task.service.utils;

import com.tapdata.tm.task.utils.TaskServiceUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTaskServiceUtils {

    @Test
    public void testGetTransformProcess() {
        double transformProcess = TaskServiceUtils.getTransformProcess(10, 20);
        Assertions.assertEquals(1, transformProcess);
    }

    @Test
    public void testGetTransformProcessBoundaryCase1() {
        double transformProcess = TaskServiceUtils.getTransformProcess(10, 0);
        Assertions.assertEquals(0, transformProcess);
    }

    @Test
    public void testGetTransformProcessBoundaryCase2() {
        double transformProcess = TaskServiceUtils.getTransformProcess(0, 1);
        Assertions.assertEquals(1, transformProcess);
    }

    @Test
    public void testGetTransformProcessBoundaryCase3() {
        double transformProcess = TaskServiceUtils.getTransformProcess(10, 5);
        Assertions.assertEquals(0.5, transformProcess);
    }
}
