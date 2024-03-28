package com.tapdata.tm.function.inspect.repository;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

class InspectFunctionRepositoryTest {
    @Test
    void testInitNormal() {
        InspectFunctionRepository repository = mock(InspectFunctionRepository.class);
        doCallRealMethod().when(repository).init();
        Assertions.assertDoesNotThrow(repository::init);
    }
}