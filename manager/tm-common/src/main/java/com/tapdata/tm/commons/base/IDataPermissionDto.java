package com.tapdata.tm.commons.base;

import java.util.Set;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/11 11:59 Create
 */
public interface IDataPermissionDto {
	Set<String> getPermissionActions();

	void setPermissionActions(Set<String> permissionActions);
}
