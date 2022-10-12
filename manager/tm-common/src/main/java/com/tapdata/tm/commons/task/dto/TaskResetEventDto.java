package com.tapdata.tm.commons.task.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import org.bson.types.ObjectId;

/**
 * @author samuel
 * @Description
 * @create 2022-10-12 12:15
 **/
public class TaskResetEventDto extends BaseDto {
	private ObjectId taskId;
	private String describe;
}
