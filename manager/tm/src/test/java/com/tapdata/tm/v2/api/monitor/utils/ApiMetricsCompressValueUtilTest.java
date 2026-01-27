package com.tapdata.tm.v2.api.monitor.utils;

import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import org.junit.jupiter.api.Nested;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiMetricsCompressValueUtilTest {

    @Nested
    class CheckByCodeTest {
        @org.junit.jupiter.api.Test
        void testCheckByCode() {
            boolean result = ApiMetricsCompressValueUtil.checkByCode("200", ApiCallEntity.HttpStatusType.PUBLISH_FAILED_404.getCode());
            assertTrue(result);
        }

        @org.junit.jupiter.api.Test
        void testCheckByCodeWithNon200() {
            boolean result = ApiMetricsCompressValueUtil.checkByCode("500", ApiCallEntity.HttpStatusType.PUBLISH_FAILED_404.getCode());
            assertFalse(result);
        }

        @org.junit.jupiter.api.Test
        void testCheckByCodeWithNull() {
            boolean result = ApiMetricsCompressValueUtil.checkByCode(null, ApiCallEntity.HttpStatusType.PUBLISH_FAILED_404.getCode());
            assertFalse(result);
        }

        @org.junit.jupiter.api.Test
        void testCheckByCodeWithEmpty() {
            boolean result = ApiMetricsCompressValueUtil.checkByCode("-", ApiCallEntity.HttpStatusType.PUBLISH_FAILED_404.getCode());
            assertFalse(result);
        }
    }
}