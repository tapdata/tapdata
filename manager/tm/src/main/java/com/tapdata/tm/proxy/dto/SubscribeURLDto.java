package com.tapdata.tm.proxy.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author GavinXiao
 * @description SubscribeURLDto create by Gavin
 * @create 2023/6/14 15:15
 **/
@EqualsAndHashCode(callSuper = true)
@Data
public class SubscribeURLDto extends SubscribeDto {
    private String randomId;
}
