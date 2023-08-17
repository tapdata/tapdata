package com.tapdata.tm.commons.base;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Set;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/8 12:14 Create
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DataPermissionAction implements Serializable {

	private String type;
	private String typeId;
	private Set<String> actions;

}
