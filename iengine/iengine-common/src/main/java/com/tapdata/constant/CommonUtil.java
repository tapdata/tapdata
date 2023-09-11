package com.tapdata.constant;

import java.math.BigDecimal;

public class CommonUtil {

	/**
	 * 比较版本号的大小,前者大则返回一个正数,后者大返回一个负数,相等则返回0
	 *
	 * @param version1
	 * @param version2
	 * @return
	 */
	public static int compareVersion(String version1, String version2) throws Exception {
		if (version1 == null || version2 == null) {
			throw new Exception("compareVersion error:illegal params.");
		}
		String[] versionArray1 = version1.split("\\.");//注意此处为正则匹配，不能用"."；
		String[] versionArray2 = version2.split("\\.");
		int idx = 0;
		int minLength = Math.min(versionArray1.length, versionArray2.length);//取最小长度值
		int diff = 0;
		while (idx < minLength
				&& (diff = versionArray1[idx].length() - versionArray2[idx].length()) == 0//先比较长度
				&& (diff = versionArray1[idx].compareTo(versionArray2[idx])) == 0) {//再比较字符
			++idx;
		}
		//如果已经分出大小，则直接返回，如果未分出大小，则再比较位数，有子版本的为大；
		diff = (diff != 0) ? diff : versionArray1.length - versionArray2.length;
		return diff;
	}

	public static int compareObjects(Object[] val1, Object[] val2) {
		// First, check if both arrays are null
		if (val1 == null && val2 == null) {
			return 0; // Both are null, considered equal
		} else if (val1 == null) {
			return -1; // Only val1 is null, considered less than val2
		} else if (val2 == null) {
			return 1; // Only val2 is null, considered greater than val1
		}

		// Next, compare the lengths of the two arrays
		int len1 = val1.length;
		int len2 = val2.length;

		if (len1 < len2) {
			return -1; // Length of val1 is less than val2, considered less than val2
		} else if (len1 > len2) {
			return 1; // Length of val1 is greater than val2, considered greater than val2
		}

		// If lengths are equal, compare elements one by one
		for (int i = 0; i < len1; i++) {
			Object obj1 = val1[i];
			Object obj2 = val2[i];

			if (obj1 == null && obj2 == null) {
				continue; // Both are null, compare the next element
			} else if (obj1 == null) {
				return -1; // Only obj1 is null, considered less than obj2
			} else if (obj2 == null) {
				return 1; // Only obj2 is null, considered greater than obj1
			}

			// Compare non-null elements using the compareTo method, assuming they implement the Comparable interface
			if (obj1 instanceof Comparable<?> && obj2 instanceof Comparable<?>) {
				if (obj1 instanceof Number) {
					obj1 = new BigDecimal(obj1.toString());
				}
				if (obj2 instanceof Number) {
					obj2 = new BigDecimal(obj2.toString());
				}
				int result = ((Comparable<Object>) obj1).compareTo(obj2);
				if (result != 0) {
					return result; // If the comparison result is not 0, return the result
				}
			} else {
				// If elements are not comparable, you can consider other comparison strategies
				// Here, you can customize based on the specific scenario
				// For example, you can compare string representations using the toString method
				int result = obj1.toString().compareTo(obj2.toString());
				if (result != 0) {
					return result;
				}
			}
		}

		return 0; // Both arrays are equal
	}
}
