package com.tapdata.tm.mcp.agent;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.openai.OpenAiChatModel;
import org.springframework.ai.openai.OpenAiChatOptions;
import org.springframework.stereotype.Component;

import java.net.Proxy;

@Component
public class SpringAiChatClientFactory implements AiChatClientFactory {

    @Override
    public ChatClient create(LlmConfig config) {
        OpenAiChatModel chatModel = OpenAiChatModel.builder()
                .options(buildOptions(config))
                .build();

        return ChatClient.builder(chatModel).build();
    }

    OpenAiChatOptions buildOptions(LlmConfig config) {
        OpenAiChatOptions.Builder optionsBuilder = OpenAiChatOptions.builder()
                .baseUrl(config.normalizedBaseUrl())
                .apiKey(config.normalizedApiKey())
                .model(config.normalizedModel())
                .streamUsage(false)
                .proxy(Proxy.NO_PROXY);
        if (config.getTemperature() != null) {
            optionsBuilder.temperature(config.getTemperature());
        }
        if (config.getMaxTokens() != null && config.getMaxTokens() > 0) {
            optionsBuilder.maxTokens(config.getMaxTokens());
        }

        return optionsBuilder.build();
    }
}
