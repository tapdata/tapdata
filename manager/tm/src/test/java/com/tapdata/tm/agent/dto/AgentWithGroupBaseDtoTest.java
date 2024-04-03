package com.tapdata.tm.agent.dto;


import com.tapdata.tm.base.exception.BizException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

class AgentWithGroupBaseDtoTest {
    AgentWithGroupBaseDto dto;
    @BeforeEach
    void init() {
        dto = mock(AgentWithGroupBaseDto.class);
    }
    @Nested
    class VerifyTest {
        @BeforeEach
        void init() {
            doCallRealMethod().when(dto).verify();
        }
        void assertVerify(String agentId, String groupId) {
            ReflectionTestUtils.setField(dto, "agentId", agentId);
            ReflectionTestUtils.setField(dto, "groupId", groupId);
            dto.verify();
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> assertVerify("id", "id"));
        }

        @Test
        void testGroupIdIsNull() {
            Assertions.assertThrows(BizException.class, () -> assertVerify(null, "id"));
        }
        @Test
        void testGroupIdIsEmpty() {
            Assertions.assertThrows(BizException.class, () -> assertVerify("", "id"));
        }

        @Test
        void testAgentIdIsNull() {
            Assertions.assertThrows(BizException.class, () -> assertVerify("id", null));
        }
        @Test
        void testAgentIdIsEmpty() {
            Assertions.assertThrows(BizException.class, () -> assertVerify("id", ""));
        }
    }
}