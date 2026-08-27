package com.tapdata.tm.dql;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DqlEnumContractTest {

    @Test
    void parsesRoutingContractCaseInsensitively() {
        assertEquals(DqlExceptionScopeEnum.RECORD, DqlExceptionScopeEnum.parse("record"));
        assertEquals(DqlExceptionScopeEnum.TASK_SHARED, DqlExceptionScopeEnum.parse("TASK_SHARED"));
        assertEquals(DqlRouteDecisionEnum.RECORD_DLQ, DqlRouteDecisionEnum.parse("record_dlq"));
        assertEquals(DqlRouteDecisionEnum.TASK_RETRY, DqlRouteDecisionEnum.parse("TASK_RETRY"));
        assertEquals(DqlClassificationConfidenceEnum.UNKNOWN_SINGLE,
                DqlClassificationConfidenceEnum.parse("unknown_single"));
    }

    @Test
    void returnsNullForMissingOrUnknownValues() {
        assertNull(DqlExceptionScopeEnum.parse(null));
        assertNull(DqlRouteDecisionEnum.parse(""));
        assertNull(DqlClassificationConfidenceEnum.parse("UNSUPPORTED"));
    }

    @Test
    void mapsLegacyTargetWriteErrorToTargetConstraintError() {
        assertEquals(DqlErrorTypeEnum.TARGET_CONSTRAINT_ERROR,
                DqlErrorTypeEnum.parse("TARGET_CONSTRAINT_ERROR"));
        assertEquals(DqlErrorTypeEnum.TARGET_CONSTRAINT_ERROR,
                DqlErrorTypeEnum.parse("TARGET_WRITE_ERROR"));
    }
}
