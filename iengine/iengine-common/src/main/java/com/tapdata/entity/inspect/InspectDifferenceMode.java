package com.tapdata.entity.inspect;

/**
 * 差异结果模式
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/12 11:02 Create
 */
public enum InspectDifferenceMode {
	All, // 输出所有差异
	OnSourceExists, // 只输出源存在的数据差异
	;

	public static boolean isOnSourceExists(String val) {
		return OnSourceExists.name().equals(val);
	}

	public static boolean isAll(String val) {
		try {
			return All == valueOf(val);
		} catch (Exception e) {
			return true;
		}
	}
}
