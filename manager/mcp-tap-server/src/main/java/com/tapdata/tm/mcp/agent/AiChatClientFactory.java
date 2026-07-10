package com.tapdata.tm.mcp.agent;

import org.springframework.ai.chat.client.ChatClient;

public interface AiChatClientFactory {

    ChatClient create(LlmConfig config);
}
