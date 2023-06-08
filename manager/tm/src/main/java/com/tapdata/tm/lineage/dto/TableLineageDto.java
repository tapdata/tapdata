package com.tapdata.tm.lineage.dto;

import com.tapdata.tm.commons.task.dto.Dag;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author samuel
 * @Description
 * @create 2023-05-19 12:00
 **/
@Data
@AllArgsConstructor
public class TableLineageDto {
	private Dag dag;
}
