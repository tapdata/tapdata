package com.tapdata.tm.message.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2023/7/1 17:11
 */
@EqualsAndHashCode(callSuper = true)
@Document("Blacklist")
@Data
public class BlacklistEntity extends BaseEntity {

    private String type; // mail, phone, regex
    private String email; // 邮箱黑名单
    private String phone; // 短信黑名单
    private String countryCode; // 电话国家代码
    private String expression;

    private boolean enable; // 是否启用白名单

}
