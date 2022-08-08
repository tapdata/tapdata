package com.tapdata.tm.user.param;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;


@AllArgsConstructor
@Getter
@Setter
public class ResetPasswordParam {
    private String email;
    private String newPassword;
    private String validateCode;
    private String location_origin;

}
