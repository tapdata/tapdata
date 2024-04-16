package com.tapdata.tm.commons.task.dto;

import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class ParentTaskDtoTest {
    ParentTaskDto dto;
    @BeforeEach
    void init() {
        dto = mock(ParentTaskDto.class);
    }

    @Test
    void testParams() {
        Assertions.assertEquals("initial_sync", ParentTaskDto.TYPE_INITIAL_SYNC);
        Assertions.assertEquals("initial_sync+cdc", ParentTaskDto.TYPE_INITIAL_SYNC_CDC);
        Assertions.assertEquals("cdc", ParentTaskDto.TYPE_CDC);
    }

    @Nested
    class GetAccessNodeProcessIdListTest {
        String accessNodeProcessId;
        String accessNodeType;
        @BeforeEach
        void init() {
            accessNodeType = AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name();
            accessNodeProcessId = "id";
            when(dto.getAccessNodeProcessIdList()).thenCallRealMethod();
        }

        void assertVerify(int exceptedSize) {
            ReflectionTestUtils.setField(dto, "accessNodeProcessId", accessNodeProcessId);
            ReflectionTestUtils.setField(dto, "accessNodeType", accessNodeType);
            List<String> processIdList = dto.getAccessNodeProcessIdList();
            Assertions.assertNotNull(processIdList);
            Assertions.assertEquals(exceptedSize, processIdList.size());
            Assertions.assertEquals(ArrayList.class.getName(), processIdList.getClass().getName());
        }

        @Test
        void testAccessNodeTypeIsUserManuallyAndAccessNodeProcessIdNotEmpty() {
            assertVerify(1);
        }
        @Test
        void testAccessNodeTypeNotUserManuallyButAccessNodeProcessIdNotEmpty() {
            accessNodeType = AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name();
            assertVerify(0);
        }
        @Test
        void testAccessNodeTypeIsUserManuallyButAccessNodeProcessIdIsEmpty() {
            accessNodeProcessId = null;
            assertVerify(0);
        }

    }
}