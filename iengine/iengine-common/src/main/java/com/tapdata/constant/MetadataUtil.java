package com.tapdata.constant;

import org.apache.commons.lang3.StringUtils;

public class MetadataUtil {

	public static String formatQualifiedName(String qualifiedName) {
		if (StringUtils.isNotBlank(qualifiedName)) {
			qualifiedName = qualifiedName.replaceAll("[/|\\.|@|\\&|:|\\?|%|=_ ]+", "_");
		}
		return qualifiedName;
	}
}
