package com.tapdata.tm.license.dto;

import lombok.Data;

import java.util.List;

@Data
public class LicenseUpdateDto {

    private LicenseUpdateReqDto reqDto;

    private String salt;

    private String sid;

    private List<String> tapdata;

    private ValidityPeriod validity_period;

    @Data
    public static class ValidityPeriod {
        private Long issued_on;
        private Long expires_on;
    }
}
