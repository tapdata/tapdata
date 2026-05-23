package com.tapdata.tm.base.controller;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.security.LoginUserResolver;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.utils.MessageUtil;
import io.tapdata.entity.simplify.TapSimplify;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import jakarta.servlet.http.HttpServletRequest;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/11 4:11 下午
 * @description
 */
@Slf4j
public class BaseController {

	@Autowired
	private LoginUserResolver loginUserResolver;

	protected boolean isAgentReq() {
		ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
		HttpServletRequest request = attributes.getRequest();
		String userAgent = request.getHeader("user-agent");
		return org.apache.commons.lang3.StringUtils.isNotBlank(userAgent) && (userAgent.contains("Java") || userAgent.contains("Node") || userAgent.contains("FlowEngine"));
	}

	public Filter parseFilter(String filterJson) {
		filterJson=replaceLoopBack(filterJson);
		Filter filter = JsonUtil.parseJson(filterJson, Filter.class);
		if (filter == null) {
			return new Filter();
		}
		Where where = filter.getWhere();
		if (where != null) {
			where.remove("user_id");
		}
		return filter;
	}

	public static Where parseWhere(String whereJson) {
		whereJson=replaceLoopBack(whereJson);
		Where where = JsonUtil.parseJson(whereJson, Where.class);
		if (where != null) {
			where.remove("user_id");
		}
		return where;
	}

	public static String replaceLoopBack(String json) {
		if (StringUtils.isNotBlank(json)) {
			json = json.replace("\"like\"", "\"$regex\"");
			json = json.replace("\"options\"", "\"$options\"");
			json = json.replace("\"$inq\"", "\"$in\"");
			json = json.replace("\"inq\"", "\"$in\"");
			json = json.replace("\"in\"", "\"$in\"");
			json = json.replace("\"neq\"", "\"$ne\"");
		}
		return json;
	}

	public Field parseField(String fieldJson) {
		return JsonUtil.parseJson(fieldJson, Field.class);
	}
	public UserDetail getLoginUser() {
		return getLoginUser(null);
	}
	public UserDetail getLoginUser(String specifiedUserId) {
		ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
		HttpServletRequest request = attributes.getRequest();
		return loginUserResolver.resolve(request, specifiedUserId);
	}

	/**
	 * Request process success.
	 * @param data return data.
	 * @param <T>  return data type.
	 * @return response result dto.
	 */
	public <T> ResponseMessage<T> success(T data) {
		ResponseMessage<T> res = new ResponseMessage<>();
		res.setData(data);
		return res;
	}

	/**
	 * Request process success.
	 * @param <T>  return data type.
	 * @return response result dto.
	 */
	public <T> ResponseMessage<T> success() {
		ResponseMessage<T> res = new ResponseMessage<>();
		return res;
	}

	/**
	 * Request process failed.
	 * @param errorCode error code.
	 * @param <T> error return data type.
	 * @return response result dto.
	 */
	public <T> ResponseMessage<T> failed(String errorCode) {
		ResponseMessage<T> res = new ResponseMessage<>();
		res.setCode(errorCode);
		res.setMessage(MessageUtil.getMessage(errorCode));
		return res;
	}

	/**
	 * Request process failed.
	 * @param errorCode error code.
	 * @param msg error message.
	 * @param <T> error return data type.
	 * @return response result dto.
	 */
	public <T> ResponseMessage<T> failed(String errorCode, String msg) {
		ResponseMessage<T> res = new ResponseMessage<>();
		res.setCode(errorCode);
		res.setMessage(msg);
		return res;
	}
	public <T> ResponseMessage<T> failed(String errorCode, String msg, Throwable e) {
		ResponseMessage<T> res = new ResponseMessage<>();
		res.setCode(errorCode);
		res.setMessage(msg);
		res.setStack(TapSimplify.getStackTrace(e));
		return res;
	}

	/**
	 * Request process failed.
	 * @param errorCode error code.
	 * @param e exception.
	 * @param <T> error return data type.
	 * @return response result dto.
	 */
	public <T> ResponseMessage<T> failed(String errorCode, Throwable e) {
		ResponseMessage<T> res = new ResponseMessage<>();
		res.setCode(errorCode);
		res.setMessage(e != null ? e.getMessage() : null);
		res.setStack(TapSimplify.getStackTrace(e));
		return res;
	}

	/**
	 * Request process failed.
	 * @param e exception.
	 * @param <T> error return data type.
	 * @return response result dto.
	 */
	public <T> ResponseMessage<T> failed(Throwable e) {
		ResponseMessage<T> res = new ResponseMessage<>();
		res.setCode("SystemError");
		res.setMessage(e != null ? e.getMessage() : null);
		res.setStack(TapSimplify.getStackTrace(e));
		return res;
	}

}
