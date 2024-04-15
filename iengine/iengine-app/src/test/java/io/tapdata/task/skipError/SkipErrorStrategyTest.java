package io.tapdata.task.skipError;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SkipErrorStrategyTest {
    @Test
    void testGetSkipErrorStrategyTypeIsNull(){
        Assertions.assertEquals("errorMessage",SkipErrorStrategy.getSkipErrorStrategy(null).getType());
    }

    @Test
    void testGetSkipErrorStrategy(){
        Assertions.assertEquals("errorMessage",SkipErrorStrategy.getSkipErrorStrategy("errorMessage").getType());
    }

    @Test
    void testGetSkipErrorStrategyDefault(){
        Assertions.assertEquals("errorMessage",SkipErrorStrategy.getSkipErrorStrategy("test").getType());
    }

    @Test
    void testGetDefaultSkipErrorStrategy(){
        Assertions.assertEquals("errorMessage",SkipErrorStrategy.getDefaultSkipErrorStrategy().getType());
    }
}
