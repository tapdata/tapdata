package com.tapdata.tm.commons.base;

import java.util.List;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/11 12:00 Create
 */
public interface IDataPermissionEntity {
	List<DataPermissionAction> getPermissions();

	void setPermissions(List<DataPermissionAction> permissions);
}
