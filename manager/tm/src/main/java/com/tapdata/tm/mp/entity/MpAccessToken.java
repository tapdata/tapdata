package com.tapdata.tm.mp.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2023/1/17 下午3:41
 */
@EqualsAndHashCode(callSuper = true)
@Document("MpAccessToken")
@Data
public class MpAccessToken extends BaseEntity {

    @Indexed(unique = true)
    private String name;

    //获取到的凭证
    private String accessToken;

    //凭证有效时间，单位：秒
    private Integer expiresIn;

    // 凭证过期时间戳
    private Long expiresAt;
}
