package com.tapdata.tm.task.param;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableLogCollectorParam {

	private String connectionId;

	private Set<String> tableNames;
}
