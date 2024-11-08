package com.tapdata.tm.commons.schema;

import io.tapdata.entity.schema.TapTable;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author samuel
 * @Description
 * @create 2024-10-31 15:27
 **/
@Data
@AllArgsConstructor
public class TapTableDto {
	private String qualifiedName;
	private TapTable tapTable;
}
