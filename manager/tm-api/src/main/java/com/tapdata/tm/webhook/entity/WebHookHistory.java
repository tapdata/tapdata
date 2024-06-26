package com.tapdata.tm.webhook.entity;

import com.tapdata.tm.base.entity.BaseEntity;
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
@Document(collection = "WebHookHistory")
public class WebHookHistory extends BaseEntity {
    private String hookId;
    private List<HookOneHistory> hookEvent;
}
