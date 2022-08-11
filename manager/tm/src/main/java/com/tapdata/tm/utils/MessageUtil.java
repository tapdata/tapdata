package com.tapdata.tm.utils;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.springframework.data.util.Streamable;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.stream.Collectors;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/13 2:42 下午
 * @description
 */
public class MessageUtil {

	private static ResourceBundle getResourceBundle(Locale locale){
		if (locale == null)
			locale = Locale.CHINA;
		return ResourceBundle.getBundle("messages", locale);
	}

	/**
	 * 根据指定的 {@code resourceId} 获取消息，并使用指定的参数 {@code params} 对消息信息进行格式化
	 * @param resourceId 资源ID
	 * @param params 格式化参数
	 * @return 格式化后的消息信息
	 * */
	public static String getMessage(String resourceId, Object... params){
		Locale locale = null;
		try {
			RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
			if (requestAttributes != null){
				locale = WebUtils.getLocale(((ServletRequestAttributes) requestAttributes).getRequest());
			}
		}catch (Exception ignored){

		}
		if (locale == null){
			locale = Locale.getDefault();
		}
		return getMessage(locale, resourceId, params);
	}

	public static Locale getLocale(){
		Locale locale = null;

		RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
		if (requestAttributes != null){
			locale = WebUtils.getLocale(((ServletRequestAttributes) requestAttributes).getRequest());
		}

		if (locale == null){
			locale = new Locale("en", "US");
		}

		return locale;
	}

	public static String getLanguage(){
		String language = MessageUtil.getLocale().toLanguageTag();
		language = StringUtils.replace(language, "-", "_");
		return language;
	}

	/**
	 * 根据指定的 {@code resourceId} 获取消息，并使用指定的参数 {@code params} 对消息信息进行格式化
	 * @param locale 本地环境
	 * @param resourceId 资源ID
	 * @param params 格式化参数
	 * @return 格式化后的消息信息
	 * */
	public static String getMessage(Locale locale, String resourceId, Object... params){
		String msg = getStringOrNull(getResourceBundle(locale), resourceId);
		if(msg == null) {
			if (params != null && params.length > 0){
				String paramsString =Streamable.of(params).stream().map(Object::toString).collect(Collectors.joining(","));
				return resourceId + ": " + paramsString;
			}
			return resourceId;
		} if(params != null && params.length > 0){
			MessageFormat messageFormat = createMessageFormat(msg, locale);
			return messageFormat.format(params);
		} else
			return msg;

	}

	protected static String getStringOrNull(ResourceBundle bundle, String key){
		try{
			return bundle.getString(key);
		} catch (Exception e){
			return null;
		}
	}

	/**
	 * Create a MessageFormat for the given message and Locale.
	 * @param msg the message to insert a MessageFormat for
	 * @param locale the Locale to insert a MessageFormat for
	 * @return the MessageFormat instance
	 */
	public static MessageFormat createMessageFormat(String msg, Locale locale) {
		return new MessageFormat((msg != null ? msg : ""), locale);
	}

	public static String formatString(String template, Map<String, Object> params){
		StringSubstitutor stringSub = new StringSubstitutor(params);
		stringSub.setVariablePrefix('{');
		return stringSub.replace(template);
	}
}
