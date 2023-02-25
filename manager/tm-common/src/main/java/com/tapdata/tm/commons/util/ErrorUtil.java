package com.tapdata.tm.commons.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author samuel
 * @Description
 * @create 2022-11-17 16:37
 **/
public class ErrorUtil {
	public static String getStackString(Throwable throwable) {
		StringWriter sw = new StringWriter();
		try (
				PrintWriter pw = new PrintWriter(sw)
		) {
			throwable.printStackTrace(pw);
			return sw.toString();
		}
	}
}
