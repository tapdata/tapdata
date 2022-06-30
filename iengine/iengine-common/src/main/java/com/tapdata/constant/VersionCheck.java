package com.tapdata.constant;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VersionCheck {

	private final static char[] VAR2_KEY = {'6', '8', '0', '1', '0', 'e', '8', '5', 'a', '8', 'c', 'd', '4', '9', '5', 'd', '3', 'a', '1', '7', '5', '7', 'e', '2', '4', '2', 'e', 'e', 'b', '0', 'd', '0'};

	private final static String AGENT_PREFIX = "tapdata-agent-";

	public static boolean check(String key) throws NoSuchAlgorithmException {
		if (StringUtils.isNotBlank(key)) {
			String[] keys = key.split(",");
			if (keys.length == 3) {

			}
		}
		return VAR2_KEY.toString().equals(key);
	}

	private static String getAgentVersionByJarName(String jarName) {
		String version = "";
		if (StringUtils.isNotBlank(jarName)
				&& StringUtils.startsWithIgnoreCase(jarName, AGENT_PREFIX)
				&& StringUtils.endsWithIgnoreCase(jarName, ".jar")) {
			version = StringUtils.removeStartIgnoreCase(jarName, AGENT_PREFIX);
			version = StringUtils.removeEndIgnoreCase(version, ".jar");
		}
		return version;
	}

	public static String getVersion() {
		String version;
		String classPath = System.getProperty("java.class.path");
		String separator = System.getProperty("path.separator");
		String jarName = "";
		if (classPath.contains(separator)) {
			String[] split = classPath.split(separator);
			for (String s : split) {
				if (s.contains(AGENT_PREFIX)) {
					jarName = s;
					break;
				}
			}
		} else {
			jarName = classPath;
		}
		if (jarName.contains(File.separator)) {
			String[] split = jarName.split("\\".equals(File.separator) ? "\\\\" : File.separator);
			jarName = split[split.length - 1];
		}
		version = VersionCheck.getAgentVersionByJarName(jarName);
		return StringUtils.isNoneBlank(version) ? version.trim() : "-";
	}

	public static void main(String[] args) {
		String ss = "user=admin@admin.com,count=1,signature=".replaceAll(" ", "").replaceAll("\\n", "");
		String substring = ss.substring(ss.indexOf("=") + 1, ss.indexOf(","));
		char[] chars = substring.toCharArray();
		StringBuilder sbkey = new StringBuilder();
		for (int i = 0; i < chars.length; i++) {
			if (i % 2 == 0) {
				sbkey.append(chars[i]);
			}
		}
//        ss.substring(chars)

		Pattern pattern = Pattern.compile("user=(.*?),");
		Matcher m = pattern.matcher(ss);

		while (m.find()) {
			System.out.println(m.group(1));
		}
	}
}
