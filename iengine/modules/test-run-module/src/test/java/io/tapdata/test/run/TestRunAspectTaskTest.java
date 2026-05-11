package io.tapdata.test.run;

import io.tapdata.entity.simplify.pretty.ClassHandlers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

public class TestRunAspectTaskTest {
    @Test
    void testTestRunAspectTask(){
        TestRunAspectTask testRunAspectTask = new TestRunAspectTask();
        ClassHandlers classHandlers = (ClassHandlers)ReflectionTestUtils.getField(testRunAspectTask, "observerClassHandlers");
        Assertions.assertEquals(1, classHandlers.keyList().size());

    }
}
