package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.entity.SamlReplayRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SamlReplayCacheServiceImplTest {

    private SamlReplayCacheServiceImpl service;

    @Mock
    private MongoTemplate mongoTemplate;

    @BeforeEach
    void setUp() {
        service = new SamlReplayCacheServiceImpl();
        ReflectionTestUtils.setField(service, "mongoTemplate", mongoTemplate);
    }

    @Test
    @DisplayName("first use inserts and returns true")
    void firstUse() {
        when(mongoTemplate.insert(any(SamlReplayRecord.class))).thenReturn(new SamlReplayRecord());
        assertTrue(service.recordIfFirstUse("assertion", "id-1"));
    }

    @Test
    @DisplayName("duplicate insert (replay) returns false")
    void replay() {
        doThrow(new DuplicateKeyException("dup")).when(mongoTemplate).insert(any(SamlReplayRecord.class));
        assertFalse(service.recordIfFirstUse("assertion", "id-1"));
    }

    @Test
    @DisplayName("blank id is treated as replay (cannot guarantee single use)")
    void blankId() {
        assertFalse(service.recordIfFirstUse("assertion", "  "));
    }
}
