package com.tapdata.tm.ds.utils;

import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DataSourceServiceUtilTest {

    @Nested
    class SetAccessNodeInfoFromOldConnectionDtoTest {
        DataSourceConnectionDto oldConnection;
        DataSourceConnectionDto update;
        @Test
        void testNormal() {
            oldConnection = new DataSourceConnectionDto();
            update = new DataSourceConnectionDto();
            update.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
            update.setAccessNodeProcessId("id");
            DataSourceServiceUtil.setAccessNodeInfoFromOldConnectionDto(oldConnection, update);
            Assertions.assertEquals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), update.getAccessNodeType());
            Assertions.assertEquals("id", update.getAccessNodeProcessId());
        }
        @Test
        void testOldConnectionIsNull() {
            oldConnection = null;
            update = new DataSourceConnectionDto();
            DataSourceServiceUtil.setAccessNodeInfoFromOldConnectionDto(oldConnection, update);
            Assertions.assertNull(update.getAccessNodeType());
            Assertions.assertNull(update.getAccessNodeProcessId());
        }
        @Test
        void tesUpdateIsNull() {
            oldConnection = new DataSourceConnectionDto();
            Assertions.assertDoesNotThrow(() -> DataSourceServiceUtil.setAccessNodeInfoFromOldConnectionDto(oldConnection, null));
        }
        @Test
        void testUpdateAccessNodeTypeIsNull() {
            oldConnection = new DataSourceConnectionDto();
            update = new DataSourceConnectionDto();
            update.setAccessNodeProcessId("id");
            DataSourceServiceUtil.setAccessNodeInfoFromOldConnectionDto(oldConnection, update);
            Assertions.assertEquals(oldConnection.getAccessNodeType(), update.getAccessNodeType());
            Assertions.assertEquals("id", update.getAccessNodeProcessId());
        }
        @Test
        void testUpdateAccessNodeProcessIdIsNull() {
            oldConnection = new DataSourceConnectionDto();
            update = new DataSourceConnectionDto();
            update.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
            DataSourceServiceUtil.setAccessNodeInfoFromOldConnectionDto(oldConnection, update);
            Assertions.assertEquals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), update.getAccessNodeType());
            Assertions.assertEquals(oldConnection.getAccessNodeProcessId(), update.getAccessNodeProcessId());
        }
    }
}