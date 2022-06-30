package com.tapdata.tm.metrics.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-12-07 17:52
 **/
@EqualsAndHashCode(callSuper = true)
@Data
@Document("Metrics")
public class MetricsEntity extends BaseEntity {

	private String name;
	private String type;
	private Double value;
	private Long ts;
	private String help;
	private Map<String, String> labels;
}
