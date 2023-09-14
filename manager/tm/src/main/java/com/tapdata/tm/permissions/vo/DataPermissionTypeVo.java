package com.tapdata.tm.permissions.vo;

import com.tapdata.tm.permissions.constants.DataPermissionTypeEnums;
import lombok.Data;

import java.io.Serializable;
import java.util.Set;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/23 17:52 Create
 */
@Data
public class DataPermissionTypeVo implements Serializable {
	private DataPermissionTypeEnums type;
	private Set<String> ids;

	public DataPermissionTypeVo(DataPermissionTypeEnums type, Set<String> ids) {
		this.type = type;
		this.ids = ids;
	}
}
