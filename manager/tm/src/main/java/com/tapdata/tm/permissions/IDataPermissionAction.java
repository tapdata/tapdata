package com.tapdata.tm.permissions;

import java.util.LinkedHashSet;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/8 16:25 Create
 */
public interface IDataPermissionAction {
	String getCollection();
	LinkedHashSet<String> allActions();
}
