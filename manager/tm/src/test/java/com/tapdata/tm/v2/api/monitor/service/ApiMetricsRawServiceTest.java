package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.v2.api.monitor.repository.ApiMetricsRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ApiMetricsRawServiceTest {

    ApiMetricsRawService service;
    ApiMetricsRepository repository;

    @BeforeEach
    void setUp() {
        service = mock(ApiMetricsRawService.class);
        repository = mock(ApiMetricsRepository.class);
        ReflectionTestUtils.setField(service, "repository", repository);
    }

    @Nested
    class FindTest {
        @org.junit.jupiter.api.Test
        void testFind() {
            when(repository.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            Assertions.assertDoesNotThrow(() -> service.find(new Query()));
        }
    }
}