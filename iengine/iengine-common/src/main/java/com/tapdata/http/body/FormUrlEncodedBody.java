package com.tapdata.http.body;

import cn.hutool.core.net.url.UrlQuery;
import cn.hutool.core.util.StrUtil;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * application/x-www-form-urlencoded 类型请求body封装
 *
 * @author looly
 * @since 5.7.17
 */
public class FormUrlEncodedBody extends BytesBody {

	/**
	 * 创建 Http request body
	 *
	 * @param form    表单
	 * @param charset 编码
	 * @return FormUrlEncodedBody
	 */
	public static FormUrlEncodedBody create(Map<String, Object> form, Charset charset) {
		return new FormUrlEncodedBody(form, charset);
	}

	/**
	 * 构造
	 *
	 * @param form    表单
	 * @param charset 编码
	 */
	public FormUrlEncodedBody(Map<String, Object> form, Charset charset) {
		super(StrUtil.bytes(UrlQuery.of(form, true).build(charset), charset));
	}

	@Override
	public String toString() {
		final ByteArrayOutputStream result = new ByteArrayOutputStream();
		write(result);
		return result.toString();
	}
}
