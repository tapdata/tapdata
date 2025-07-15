package io.tapdata.test.run;

import io.tapdata.entity.simplify.pretty.ClassHandlersV2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

public class TestRunAspectTaskTest {
    @Test
    void testTestRunAspectTask(){
        TestRunAspectTask testRunAspectTask = new TestRunAspectTask();
        ClassHandlersV2 classHandlersV2 = (ClassHandlersV2)ReflectionTestUtils.getField(testRunAspectTask, "valueHandler");
        Assertions.assertEquals(4,classHandlersV2.keyList().size());

    }
}
