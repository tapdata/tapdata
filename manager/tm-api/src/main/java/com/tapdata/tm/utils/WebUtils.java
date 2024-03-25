package com.tapdata.tm.utils;

import com.tapdata.manager.common.utils.StringUtils;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;

public final class WebUtils {
	public static String getParameter(HttpServletRequest request, String key) {
		String parameter = request.getParameter(key);
		if(parameter == null) {
			parameter = request.getHeader(key);
		}
		if(parameter == null)
			return null;
		return parameter;
	}
	public static String getRealIpAddress(HttpServletRequest request) {
		String ip = request.getHeader("X-Forwarded-For");
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("Proxy-Client-IP");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("WL-Proxy-Client-IP");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("HTTP_CLIENT_IP");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("HTTP_X_FORWARDED_FOR");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getRemoteHost() != null ? request.getRemoteHost() : request.getRemoteAddr();
		}
		return ip;
	}

	public static Locale getLocale(HttpServletRequest request) {

		if (request.getCookies() != null) {
			Cookie langCookie = Arrays.stream(request.getCookies())
					.filter(cookie -> "lang".equalsIgnoreCase(cookie.getName())).findFirst().orElse(null);
			if (langCookie != null) {
				String lang = langCookie.getValue();
				Locale local = null;
				try {
					if(lang != null){
						lang = lang.replaceAll("_","-");
						local = Locale.forLanguageTag(lang);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				if (local != null) {
					return local;
				}
			}
		}

		Locale locale = request.getLocale();
		return locale != null ? locale : Locale.getDefault();

		/*String acceptLanguage = request.getHeader("Accept-Language");
		if (!StringUtils.isEmpty(acceptLanguage)) {
			String[] languageList = acceptLanguage.split(";");
			if (languageList.length > 0 && languageList[0].indexOf(',') > 0){
				List<String> languageAlias = Arrays.asList(languageList[0].split(","));
				languageAlias = languageAlias.stream()
					.filter( str -> str.indexOf('q') == -1)
					.sorted((a, b) -> b.length() - a.length()).collect(Collectors.toList());

				Locale local = null;
				if (languageAlias.size() > 0)
					local = Locale.forLanguageTag(languageAlias.get(0));
				if (local != null)
					return local;
			}
		}*/
	}

	public static boolean isAjaxRequest(HttpServletRequest request) {
		String a = request.getHeader("X-Requested-With");
		if (!StringUtils.isEmpty(a))
			return "XMLHttpRequest".equalsIgnoreCase(a);
		return false;
		/*return Streamable.of(request.getHeader)
				.stream().filter("X-Requested-With"::equalsIgnoreCase).findFirst()
				.map(val -> {
					String flag = request.getHeaders().getFirst(val);
					return "XMLHttpRequest".equalsIgnoreCase(flag);
				})
				.isPresent();*/
	}

	public static String getOrigin(HttpServletRequest httpServletRequest) {

		String forwardedProto = httpServletRequest.getHeader("x-forwarded-proto");
		String forwardedFor = httpServletRequest.getHeader("x-forwarded-for");
		String forwardedPort = httpServletRequest.getHeader("x-forwarded-port");
		String forwardedHost = httpServletRequest.getHeader("x-forwarded-host");
		String origin = httpServletRequest.getHeader("origin");

		StringBuilder sb = new StringBuilder();
		if (forwardedProto != null && (forwardedHost != null || forwardedFor != null)) {
			sb.append(forwardedProto).append("://");
			if (forwardedHost != null) {
				sb.append(forwardedHost);
			} else {
				sb.append(forwardedFor).append(":").append(forwardedPort);
			}
			return sb.toString();
		} else {
			return httpServletRequest.getHeader("origin");
		}
	}

	public static Map<String, List<String>> parseQueryString(String queryString) {

		Map<String, List<String>> optionsMap = new HashMap<>();
		if (queryString == null) {
			return optionsMap;
		}

		for (final String part : queryString.split("&|;")) {
			if (part.length() == 0) {
				continue;
			}
			int idx = part.indexOf("=");
			if (idx >= 0) {
				String key = part.substring(0, idx);
				String value = part.substring(idx + 1);
				List<String> valueList = optionsMap.get(key);
				if (valueList == null) {
					valueList = new ArrayList<String>(1);
				}
				valueList.add(urlDecode(value));
				optionsMap.put(key, valueList);
			} else {
				throw new IllegalArgumentException(format("The connection string contains an invalid option '%s'. "
					+ "'%s' is missing the value delimiter eg '%s=value'", queryString, part, part));
			}
		}
		return optionsMap;
	}

	public static String urlDecode(String str) {
		return str;
	}
}
