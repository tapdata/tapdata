package io.tapdata.common;

import io.tapdata.TapInterface;

public class TapInterfaceUtil {

	public static TapInterface getTapInterface(String databaseType, String sourceOrTarget) {
		TapInterface tapInterface = null;
		Class<?> clazzByDatabaseType = ClassScanner.getClazzByDatabaseType(databaseType, sourceOrTarget);
		if (clazzByDatabaseType != null) {
			try {
				tapInterface = (TapInterface) clazzByDatabaseType.newInstance();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return tapInterface;
	}
}
