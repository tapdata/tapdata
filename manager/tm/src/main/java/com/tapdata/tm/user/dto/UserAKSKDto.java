package com.tapdata.tm.user.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserAKSKDto {

    /**
     * 用户id
     */
    private String userId;

    /**
     * ak
     */
    private String accessKey;
    /**
     * sk
     */
    private String secretKey;

}
