package com.tapdata.tm.agent.util;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AgentGroupTagTest {
    @Test
    void testParams() {
        Assertions.assertEquals("is_delete", AgentGroupTag.TAG_DELETE);
        Assertions.assertEquals("name", AgentGroupTag.TAG_NAME);
        Assertions.assertEquals("groupId", AgentGroupTag.TAG_GROUP_ID);
        Assertions.assertEquals("agentIds", AgentGroupTag.TAG_AGENT_IDS);
        Assertions.assertEquals("process_id", AgentGroupTag.TAG_PROCESS_ID);
        Assertions.assertEquals("worker_type", AgentGroupTag.TAG_WORKER_TYPE);
        Assertions.assertEquals("connector", AgentGroupTag.TAG_CONNECTOR);
        Assertions.assertEquals("accessNodeType", AgentGroupTag.TAG_ACCESS_NODE_TYPE);
        Assertions.assertEquals("accessNodeProcessIdList", AgentGroupTag.TAG_ACCESS_NODE_PROCESS_ID_LIST);
        Assertions.assertEquals(60, AgentGroupTag.MAX_AGENT_GROUP_NAME_LENGTH);
        Assertions.assertEquals("group.not.fund", AgentGroupTag.GROUP_NOT_FUND);
    }
}