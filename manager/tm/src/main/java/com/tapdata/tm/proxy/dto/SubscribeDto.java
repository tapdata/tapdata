package com.tapdata.tm.proxy.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import io.tapdata.entity.serializer.JavaCustomSerializer;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.*;

@EqualsAndHashCode(callSuper = true)
@Data
public class SubscribeDto extends BaseDto {
	private String subscribeId;
	private String service;
	private Integer expireSeconds;
}
