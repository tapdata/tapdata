package com.tapdata.tm.proxy.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class SubscribeDto extends BaseDto {
    protected String supplierKey;
    protected String subscribeId;
    protected String service;
    protected Integer expireSeconds;
}
