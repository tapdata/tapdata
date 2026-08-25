package com.tapdata.processor;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScriptSandboxPolicyTest {

    @Test
    void workflowPolicyBlocksWideNetworkClasses() {
        assertFalse(ScriptUtil.isAllowedHostClass("com.tapdata.processor.util.CustomRest", null, ScriptSandboxPolicy.WORKFLOW));
        assertFalse(ScriptUtil.isAllowedHostClass("com.tapdata.http.HttpUtil", null, ScriptSandboxPolicy.WORKFLOW));
        assertFalse(ScriptUtil.isAllowedHostClass("com.tapdata.constant.NetworkUtil", null, ScriptSandboxPolicy.WORKFLOW));
        assertTrue(ScriptUtil.isAllowedHostClass("com.tapdata.constant.DateUtil", null, ScriptSandboxPolicy.WORKFLOW));
    }

    @Test
    void compatiblePolicyStillAllowsCustomRest() {
        assertTrue(ScriptUtil.isAllowedHostClass("com.tapdata.processor.util.CustomRest", null, ScriptSandboxPolicy.COMPATIBLE));
    }
}
