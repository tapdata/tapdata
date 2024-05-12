package com.tapdata.tm.webhook.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.webhook.enums.PingResult;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

/**
 * @author Gavin'Xiao
 * @date 2024/5/10
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "WebHookInfo")
public class WebHookInfo extends BaseEntity {

    private String userId;

    /**
     * WebHook name
     */
    private String hookName;

    /**
     * WebHook URL
     */
    private String url;

    /**
     * 是否启用
     */
    private Boolean open;

    /**
     * 是否默认告警方式
     */
    private Boolean defaultType;

    private String token;

    private String httpUser;

    private String httpPwd;

    private String customHttpHead;

    /**
     *
     */
    private String customTemplate;

    /**
     * ping状态
     */
    private PingResult pingResult;

    /**
     * 备注
     */
    private String mark;

    /**
     * @see com.tapdata.tm.webhook.enums.HookType hookName
     */
    private List<String> hookTypes;

    private boolean deleted;
}
