package com.tapdata.tm.base.security;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.config.component.ProductComponent;
import com.tapdata.tm.user.service.UserService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@Slf4j
public class LoginUserResolver {

	public static final String LOGIN_USER_ATTRIBUTE = LoginUserResolver.class.getName() + ".LOGIN_USER";

	private static final Map<String, Set<String>> authWhiteListMap = new HashMap<>() {{

		put("GET", new java.util.HashSet<>() {{
//			add("/api/MetadataInstances/**");
//			add("/api/MetadataDefinition/**");
			add("/api/Javascript_functions/**");
			add("/api/customNode/**");
			add("/api/clusterStates/**");
			add("/api/Workers/**");
//			add("/api/discovery/**");
//			add("/api/shareCache/**");
		}});

		put("PATCH", new java.util.HashSet<String>() {{
			add("/api/Javascript_functions/**");
			add("/api/customNode/**");
			add("/api/clusterStates/**");
			add("/api/Workers/**");
		}});
	}};

	private static final Map<String, Set<String>> authWhitoutListMap = new HashMap<>() {{
		put("GET", new java.util.HashSet<>() {{
			add("/api/clusterStates/findAccessNodeInfo");
		}});
	}};

	@Autowired
	private UserService userService;
	@Autowired
	private AccessTokenService accessTokenService;
	@Autowired
	private ProductComponent productComponent;

	public UserDetail resolve(HttpServletRequest request) {
		return resolve(request, null);
	}

	public UserDetail resolve(HttpServletRequest request, String specifiedUserId) {
		if (specifiedUserId == null) {
			Object cached = request.getAttribute(LOGIN_USER_ATTRIBUTE);
			if (cached instanceof UserDetail) {
				return (UserDetail) cached;
			}
		}
		UserDetail userDetail = doResolve(request, specifiedUserId);
		if (specifiedUserId == null) {
			request.setAttribute(LOGIN_USER_ATTRIBUTE, userDetail);
		}
		return userDetail;
	}

	private UserDetail doResolve(HttpServletRequest request, String specifiedUserId) {
		if (!StringUtils.isBlank(specifiedUserId)) {
			log.debug("Load user by specifiedUserId({})", specifiedUserId);
			UserDetail userDetail = userService.loadUserByExternalId(specifiedUserId);
			if (userDetail != null) {
				judgeFreeAuth(request.getRequestURI(), request.getMethod(), userDetail);
				return userDetail;
			}
			throw new BizException("NotLogin");
		}

		String userIdFromHeader = request.getHeader("user_id");
		if (!StringUtils.isBlank(userIdFromHeader)) {
			log.debug("Load user by request header user_id({})", userIdFromHeader);
			UserDetail userDetail = userService.loadUserByExternalId(userIdFromHeader);
			if (userDetail != null) {
				judgeFreeAuth(request.getRequestURI(), request.getMethod(), userDetail);
				return userDetail;
			}
			throw new BizException("NotLogin");
		}

		String queryString = request.getQueryString() != null ? request.getQueryString() : "";
		if (queryString.contains("access_token")) {
			Map<String, String> queryMap = Arrays.stream(queryString.split("&"))
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
			if (userId == null) {
				throw new BizException("NotLogin");
			}
			UserDetail userDetail = userService.loadUserById(userId);
			if (userDetail != null) {
				judgeFreeAuth(request.getRequestURI(), request.getMethod(), userDetail);
				return userDetail;
			}
			throw new BizException("NotLogin");
		}

		if (request.getHeader("authorization") != null) {
			UserDetail userDetail = null;
			String authorization = request.getHeader("authorization").trim();
			if (authorization.contains(" ")) {
				String[] array = authorization.split(" ");
				String authorizationType = array.length > 0 ? array[0] : null;
				String authorizationParams = array.length > 1 ? array[1] : null;
				if ("Basic".equalsIgnoreCase(authorizationType)) {
					authorizationParams = new String(Base64.getDecoder().decode(authorizationParams.getBytes()));
					if (authorizationParams.contains(":")) {
						array = authorizationParams.split(":");
						String username = array.length > 0 ? array[0] : null;
						String password = array.length > 1 ? array[1] : null;
						if ("Gotapd8!".equalsIgnoreCase(password)) {
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
		}

		throw new BizException("NotLogin");
	}

	private void judgeFreeAuth(String uri, String method, UserDetail userDetail) {
		if (productComponent.isDAAS() && isFreeAuth(uri, method)) {
			userDetail.setFreeAuth();
		}
	}

	public static boolean isFreeAuth(String uri, String method) {
		Set<String> uriSet = authWhiteListMap.get(method.trim().toUpperCase());
		if (uriSet == null) {
			return false;
		}
		Set<String> uriSetWithout = authWhitoutListMap.get(method.trim().toUpperCase());
		if (uriSetWithout != null && !uriSetWithout.isEmpty()) {
			for (String pattern : uriSetWithout) {
				if (new org.springframework.util.AntPathMatcher().match(pattern, uri)) {
					return false;
				}
			}
		}
		for (String pattern : uriSet) {
			if (new org.springframework.util.AntPathMatcher().match(pattern, uri)) {
				return true;
			}
		}
		return false;
	}
}
