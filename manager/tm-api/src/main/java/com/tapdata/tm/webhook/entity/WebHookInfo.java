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

    String userId;

    /**  WebHook name*/
    String hookName;

    /**  WebHook URL*/
    String url;

    /**  是否启用*/
    Boolean open;

    /**  http timout, default 30s*/
    Long timeout;

    /**  是否默认告警方式*/
    Boolean defaultType;

    String token;

    String httpUser;

    String httpPwd;

    String customHttpHead;

    /**  */
    String customTemplate;

    /** ping状态*/
    PingResult pingResult;

    /**备注*/
    String mark;

    /**
     * @see com.tapdata.tm.webhook.enums.HookType hookName
     * */
    List<String> hookTypes;

    boolean deleted;
}
