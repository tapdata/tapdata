package com.tapdata.tm.permissions.constants;

import com.tapdata.tm.permissions.IDataPermissionAction;

import java.util.LinkedHashSet;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/3 12:09 Create
 */
public enum DataPermissionDataTypeEnums implements IDataPermissionAction {
	Connections(
		"Connections",
		DataPermissionActionEnums.View,
		DataPermissionActionEnums.Edit,
		DataPermissionActionEnums.Delete
	), Task(
		"Task",
		DataPermissionActionEnums.View,
		DataPermissionActionEnums.Edit,
		DataPermissionActionEnums.Delete,
		DataPermissionActionEnums.Reset,
		DataPermissionActionEnums.Start,
		DataPermissionActionEnums.Stop
	),
	;
	private final String collection;
	private final LinkedHashSet<String> allActions;

	DataPermissionDataTypeEnums(String collection, DataPermissionActionEnums... allActions) {
		this.collection = collection;
		this.allActions = new LinkedHashSet<>();
		for (DataPermissionActionEnums allAction : allActions) {
			this.allActions.add(allAction.name());
		}
	}

	@Override
	public String getCollection() {
		return collection;
	}

	@Override
	public LinkedHashSet<String> allActions() {
		return new LinkedHashSet<>(allActions);
	}

	public static DataPermissionDataTypeEnums parse(String str) {
		if (null == str || str.isEmpty()) return null;

		for (DataPermissionDataTypeEnums type : values()) {
			if (type.name().equalsIgnoreCase(str)) {
				return type;
			}
		}
		return null;
	}
}
