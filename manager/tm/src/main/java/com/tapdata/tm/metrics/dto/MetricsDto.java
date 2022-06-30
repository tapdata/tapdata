package com.tapdata.tm.metrics.dto;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-12-07 17:52
 **/
@EqualsAndHashCode(callSuper = true)
@Data
public class MetricsDto extends BaseDto {

	private String name;
	private String type;
	private Double value;
	private Long ts;
	private String help;
	private Map<String, String> labels;
}
