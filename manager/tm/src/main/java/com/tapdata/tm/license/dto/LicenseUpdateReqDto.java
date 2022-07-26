package com.tapdata.tm.license.dto;

import lombok.Data;

import java.util.List;

@Data
public class LicenseUpdateReqDto {

    private List<String> sid;

    private String license;
}
