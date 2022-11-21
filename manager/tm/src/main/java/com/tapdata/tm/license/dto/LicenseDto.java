package com.tapdata.tm.license.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.commons.base.dto.BaseDto;

import java.util.Date;
import java.util.List;


/**
 * License
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class LicenseDto extends BaseDto {

    /** license 解密后的字段 start */
    private Long salt;

    private String sid;

    private List<String> tapdata;

    private ValidityPeriod validity_period;
    /** license 解密后的字段 end */

    /**数据库实体字段start 共用sid */
    private String hostname;

    private String license;

    private Long issued_on;

    private Long expires_on;

    private Date expirationDate;
    /**数据库实体字段 end */

    private String license_sid;

    @Data
    public static class ValidityPeriod {
        private Long issued_on;
        private Long expires_on;
    }

}