package com.tapdata.tm.permissions.vo;

import lombok.Data;

import java.io.Serializable;
import java.util.Set;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/14 15:21 Create
 */
@Data
public class DataPermissionAuthVo implements Serializable {
	private String type;
	private Set<String> typeIds;
	private String dataType;
	private Set<String> dataIds;
	private Set<String> actions;
}
