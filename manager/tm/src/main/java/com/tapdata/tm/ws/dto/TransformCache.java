package com.tapdata.tm.ws.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * @Author: Zed
 * @Date: 2022/3/5
 * @Description:
 */
@AllArgsConstructor
@Getter
@Setter
public class TransformCache {

    public TransformCache(String sessionId, String sender) {
        this.sender = sender;
        this.sessionId = sessionId;
    }
    private String sessionId;

    private String sender;
    private String stageId;
}
