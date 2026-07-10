package com.tapdata.tm.mcp.agent;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class AiChatRequest {

    private LlmConfig llm;
    private List<ChatMessage> messages;
    private Integer maxToolRounds;

    public void validate() {
        if (llm == null) {
            throw new IllegalArgumentException("llm is required");
        }
        llm.validate();
        if (messages == null || messages.isEmpty()) {
            throw new IllegalArgumentException("messages is required");
        }
        for (ChatMessage message : messages) {
            if (message == null || StringUtils.isBlank(message.getRole())) {
                throw new IllegalArgumentException("messages.role is required");
            }
            if (StringUtils.isBlank(message.getContent())) {
                throw new IllegalArgumentException("messages.content is required");
            }
        }
    }

    public int resolvedMaxToolRounds() {
        if (maxToolRounds == null || maxToolRounds <= 0) {
            return 6;
        }
        return Math.min(maxToolRounds, 12);
    }

    public LlmConfig getLlm() {
        return llm;
    }

    public void setLlm(LlmConfig llm) {
        this.llm = llm;
    }

    public List<ChatMessage> getMessages() {
        return messages;
    }

    public void setMessages(List<ChatMessage> messages) {
        this.messages = messages;
    }

    public Integer getMaxToolRounds() {
        return maxToolRounds;
    }

    public void setMaxToolRounds(Integer maxToolRounds) {
        this.maxToolRounds = maxToolRounds;
    }
}
