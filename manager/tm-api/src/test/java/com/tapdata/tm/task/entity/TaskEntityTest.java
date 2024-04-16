package com.tapdata.tm.task.entity;

import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.utils.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TaskEntityTest {
    TaskEntity entity;
    @BeforeEach
    void init() {
        entity = mock(TaskEntity.class);
    }

    @Nested
    class GetAccessNodeProcessIdTest {
        @BeforeEach
        void init() {
            when(entity.getAccessNodeProcessId()).thenCallRealMethod();
        }
        @Test
        void testIsGroupManually() {
            ReflectionTestUtils.setField(entity, "accessNodeType", AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
            ReflectionTestUtils.setField(entity, "accessNodeProcessId", "id");
            Assertions.assertEquals("id", entity.getAccessNodeProcessId());
        }
        @Test
        void testNotGroupManually() {
            ReflectionTestUtils.setField(entity, "accessNodeType", AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
            ReflectionTestUtils.setField(entity, "accessNodeProcessId", "id");
            ReflectionTestUtils.setField(entity, "accessNodeProcessIdList", Lists.newArrayList("xid", "yid"));
            Assertions.assertEquals("xid", entity.getAccessNodeProcessId());
        }
        @Test
        void testNotGroupManuallyAndAccessNodeProcessIdListIsEmpty() {
            ReflectionTestUtils.setField(entity, "accessNodeType", AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
            ReflectionTestUtils.setField(entity, "accessNodeProcessId", "id");
            ReflectionTestUtils.setField(entity, "accessNodeProcessIdList", Lists.newArrayList());
            Assertions.assertEquals("", entity.getAccessNodeProcessId());
        }
    }
}