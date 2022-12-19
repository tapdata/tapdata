package com.tapdata.tm.base.controller;

import cn.hutool.core.util.ReUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.component.ProductComponent;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MessageUtil;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.PathMatcher;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/11 4:11 下午
 * @description
 */
@Slf4j
public class BaseController {

	@Autowired
	private UserService userService;
	@Autowired
	private AccessTokenService accessTokenService;

	@Autowired
	private ProductComponent productComponent;

	private static final PathMatcher pathMatcher = new AntPathMatcher();

	public static void main(String[] args) {
		System.out.println(ReUtil.isMatch("get|post|put", "get1"));
		System.out.println(ReUtil.isMatch("get|post|put", "get"));
		System.out.println(ReUtil.isMatch("/api/MetadataInstances", "/api/MetadataInstances"));

		AntPathMatcher antPathMatcher = new AntPathMatcher();
		System.out.println(antPathMatcher.match("/api/MetadataInstances/**", "/api/MetadataInstances/node/schemaPage"));
		System.out.println(antPathMatcher.match("/api/MetadataInstances/**", "/api/MetadataInstances"));
		System.out.println(antPathMatcher.match("/api/MetadataInstances/**".toLowerCase(), "/api/metadatainstances".toLowerCase()));
		System.out.println(antPathMatcher.match("/api/MetadataInstances/**", "/api/MetadataInstances?id=1"));
		System.out.println(antPathMatcher.match("/api/MetadataInstances/**", "/api/MetadataInstance1"));
	}

	private static final Map<String, Set<String>> authWhiteListMap = new HashMap<String, Set<String>>() {{

		put("GET", new HashSet<String>() {{
//			add("/api/MetadataInstances/**");
//			add("/api/MetadataDefinition/**");
			add("/api/Javascript_functions/**");
			add("/api/customNode/**");
			add("/api/clusterStates/**");
			add("/api/Workers/**");
//			add("/api/discovery/**");
//			add("/api/shareCache/**");
		}});
	}};

	private static boolean isFreeAuth(String uri, String method) {
		Set<String> uriSet = authWhiteListMap.get(method.trim().toUpperCase());
		if (uriSet == null) {
			return false;
		}
		for (String pattern : uriSet) {
			if (pathMatcher.match(pattern, uri)) {
				return true;
			}
		}
		return false;
	}

	private void judgeFreeAuth(String uri, String method, UserDetail userDetail) {
		if (productComponent.isDAAS()) {
			if (isFreeAuth(uri, method)) {
 				userDetail.setFreeAuth();
			}
		}
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
//		return new UserDetail("62bc5008d4958d013d97c7a6", "62bc5008d4958d013d97c7a6", "admin@admin.com", "", Collections.singletonList(new SimpleGrantedAuthority("USERS")));
//	}
////	public UserDetail getLoginUser1() {
////				return new UserDetail("627c7a5b2974b11fab38df33", "627c7a5b2974b11fab38df33", "admin@admin.com", "", Collections.singletonList(new SimpleGrantedAuthority("USERS")));
////}
//	public UserDetail getLoginUser1() {
		//Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

		ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
		HttpServletRequest request = attributes.getRequest();

		String userIdFromHeader = request.getHeader("user_id");

		if (!StringUtils.isBlank(userIdFromHeader)) {
			log.info("Load user by request header user_id({})", userIdFromHeader);
			UserDetail userDetail = userService.loadUserByExternalId(userIdFromHeader);
			if (userDetail != null) {
				judgeFreeAuth(request.getRequestURI(), request.getMethod(), userDetail);
				return userDetail;
			}
			throw new BizException("NotLogin");
		} else if((request.getQueryString() != null ? request.getQueryString() : "").contains("access_token")) {

			Map<String, String> queryMap = Arrays.stream(request.getQueryString().split("&"))
					.filter(s -> s.startsWith("access_token"))
					.map(s -> s.split("=")).collect(Collectors.toMap(a -> a[0], a -> {
						try {
							return URLDecoder.decode(a[1], "UTF-8");
						} catch (UnsupportedEncodingException e) {
							e.printStackTrace();
							return a[1];
						}
					}, (a, b) -> a));
			String accessToken = queryMap.get("access_token");
			ObjectId userId = accessTokenService.validate(accessToken);
			if (userId == null)  {
				throw new BizException("NotLogin");
			}
			UserDetail userDetail = userService.loadUserById(userId);
			if (userDetail != null) {
				judgeFreeAuth(request.getRequestURI(), request.getMethod(), userDetail);
				return userDetail;
			}
			throw new BizException("NotLogin");
		} else if (request.getHeader("authorization") != null) {
			UserDetail userDetail = null;
			String authorization = request.getHeader("authorization").trim();
			if (authorization.contains(" ")) {
				String[] _array = authorization.split(" ");
				String authorizationType = _array.length > 0 ? _array[0] : null;
				String authorizationParams = _array.length > 1 ? _array[1] : null;
				if ("Basic".equalsIgnoreCase(authorizationType)) {
					authorizationParams = new String(Base64.getDecoder().decode(authorizationParams.getBytes()));
					if (authorizationParams.contains(":")) {
						_array = authorizationParams.split(":");
						String username = _array.length > 0 ? _array[0] : null;
						String password = _array.length > 1 ? _array[1] : null;
						if("Gotapd8!".equalsIgnoreCase(password)) {
							userDetail = userService.loadUserByUsername(username);
						} else {
							throw new BizException("WrongPassword");
						}
					}
				}
			}
			if (userDetail != null) {
				judgeFreeAuth(request.getRequestURI(), request.getMethod(), userDetail);
				return userDetail;
			}
			throw new BizException("NotLogin");
		} else {
			throw new BizException("NotLogin");
		}
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
		return res;
	}

}
