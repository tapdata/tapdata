package com.tapdata.tm.mcp.agent;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AiChatRequestTest {

    @Test
    void requiresLlmConfigAndMessages() {
        AiChatRequest request = new AiChatRequest();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, request::validate);

        assertEquals("llm is required", exception.getMessage());
    }

    @Test
    void normalizesLlmBaseUrlWithoutChatPath() throws Exception {
        LlmConfig llm = new LlmConfig();
        llm.setBaseUrl(" https://api.openai.com/v1/ ");
        llm.setApiKey("sk-test");
        llm.setModel("gpt-4.1");

        AiChatRequest request = new AiChatRequest();
        request.setLlm(llm);
        request.setMessages(List.of(new ChatMessage("user", "create a mongo connection")));

        request.validate();

        assertEquals("https://api.openai.com/v1", request.getLlm().normalizedBaseUrl());
        assertThrows(NoSuchFieldException.class, () -> LlmConfig.class.getDeclaredField("chatPath"));
    }

    @Test
    void appendsV1WhenOpenAiCompatibleBaseUrlHasNoPath() {
        LlmConfig llm = new LlmConfig();
        llm.setBaseUrl(" https://api.super-nb.me/ ");

        assertEquals("https://api.super-nb.me/v1", llm.normalizedBaseUrl());
    }
}
