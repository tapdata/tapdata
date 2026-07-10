package com.tapdata.tm.mcp.agent;

import org.junit.jupiter.api.Test;
import org.springframework.ai.openai.OpenAiChatOptions;

import java.net.Proxy;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class SpringAiChatClientFactoryTest {

    @Test
    void disablesSystemProxyByDefault() {
        SpringAiChatClientFactory factory = new SpringAiChatClientFactory();

        OpenAiChatOptions options = factory.buildOptions(llmConfig());

        assertSame(Proxy.NO_PROXY, options.getProxy());
    }

    @Test
    void disablesStreamUsageForOpenAiCompatibleProviders() {
        SpringAiChatClientFactory factory = new SpringAiChatClientFactory();

        OpenAiChatOptions options = factory.buildOptions(llmConfig());

        assertNotNull(options.getStreamOptions());
        assertFalse(options.getStreamOptions().includeUsage());
    }

    private LlmConfig llmConfig() {
        LlmConfig config = new LlmConfig();
        config.setBaseUrl("https://api.openai.com/v1");
        config.setApiKey("sk-test");
        config.setModel("gpt-4.1");
        return config;
    }
}
