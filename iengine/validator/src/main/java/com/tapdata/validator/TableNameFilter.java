package com.tapdata.validator;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class TableNameFilter {

	public static List<Pattern> convertToPatternList(String tableFilter) {
		List<Pattern> patterns = null;
		try {
			if (StringUtils.isNotBlank(tableFilter)) {
				patterns = new ArrayList<>();

				String patternStr = tableFilter.replace("*", ".*");
				patternStr = patternStr.replace("$", "\\$");
				String[] split = patternStr.split(",");
				if (split.length > 0) {
					for (String str : split) {
						try {
							if (StringUtils.isNotBlank(str)) {
								patterns.add(Pattern.compile("^" + str.trim() + "$"));
							}
						} catch (Exception e) {
							continue;
						}
					}
				}
			}
		} catch (Exception e) {
			// table filter invalid
			patterns = null;
		}

		return patterns;
	}

	public static boolean match(List<Pattern> patterns, String tableName) {
		boolean result = false;
		if (CollectionUtils.isEmpty(patterns)) {
			return true;
		}
		try {
			if (CollectionUtils.isNotEmpty(patterns)) {
				for (Pattern pattern : patterns) {
					if (pattern.matcher(tableName).find()) {
						result = true;
						break;
					}
				}
			}
		} catch (Exception e) {
			// default include
			result = false;
		}

		return result;
	}

}
