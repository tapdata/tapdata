package io.tapdata.observable.metric.handler;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JUnitDemo {
    RandomSampleEventHandler randomSampleEventHandler;

    @BeforeEach
    void beforeEach() {
        randomSampleEventHandler = mock(RandomSampleEventHandler.class);
    }

    @Nested
    class RandomSampleListTest{
        @BeforeEach
        void beforeEach() {
            when(randomSampleEventHandler.randomSampleList(any(List.class), anyDouble())).thenCallRealMethod();
        }

        @Test
        void test1() {
//            List<Object> actual = randomSampleEventHandler.randomSampleList();
//            assertNotNull(actual);
//            assertEquals(ArrayList.class, actual.getClass());
        }
    }
}
