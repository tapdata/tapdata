package com.tapdata.tm.ws.dto;

import lombok.*;

/**
 * @Author: Zed
 * @Date: 2021/11/25
 * @Description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper=false)
public class EditFlushCache {

    private String sessionId;

    private String receiver;
}
