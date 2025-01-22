package io.tapdata.flow.engine.V2.util;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SkipIdleProcessorTest {
    @Test
    void test_main(){
        Assertions.assertEquals(100, SkipIdleProcessor.SLEEP_INTERVAL);
        Assertions.assertEquals(40, SkipIdleProcessor.MAX_COUNTS);
    }

}
