package com.tapdata.tm.user.dto;

import lombok.Data;

import jakarta.validation.constraints.NotEmpty;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/2/16 下午6:34
 */
@Data
public class BindPhoneReq {
    @NotEmpty
    private String phone;

    private String phoneCountryCode;

    private boolean phoneVerified;

    private boolean bindPhone;
}
