package com.tapdata.tm.mcp.agent;

public record AgentRequestContext(String sessionId, String userId, String accessToken) {
}
