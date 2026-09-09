package com.tapdata.tm.base.security;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.config.component.ProductComponent;
import com.tapdata.tm.user.service.UserService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Set;

@Component
@Slf4j
public class LoginUserResolver {

	public static final String LOGIN_USER_ATTRIBUTE = LoginUserResolver.class.getName() + ".LOGIN_USER";

	/**
	 * 前端在被动请求（定时轮询、看板自动刷新等）上加 {@code X-User-Activity: 0} 显式声明本次请求不算用户活跃，
	 * 避免无操作期间会话被持续顺延。其它值（包括缺失该 header）一律按"活跃"处理，向后兼容老前端。
	 */
	public static final String USER_ACTIVITY_HEADER = "X-User-Activity";

	private static final Set<String> FREE_AUTH_PATTERNS = Set.of(
			"/api/Javascript_functions/**",
			"/api/customNode/**",
			"/api/clusterStates/**",
			"/api/Workers/**"
	);

	private static final Map<String, Set<String>> authWhiteListMap = Map.of(
			"GET", FREE_AUTH_PATTERNS,
			"PATCH", FREE_AUTH_PATTERNS
	);

	private static final Map<String, Set<String>> authWhitoutListMap = Map.of(
			"GET", Set.of("/api/clusterStates/findAccessNodeInfo")
	);

	@Autowired
	private UserService userService;
	@Autowired
	private AccessTokenService accessTokenService;
	@Autowired
	private ProductComponent productComponent;

	@Value("${security.auth.url-token-mode:COMPAT}")
	private String urlTokenModeValue = "COMPAT";

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

		AccessTokenResolution resolution = AccessTokenResolver.resolve(request, UrlTokenMode.from(urlTokenModeValue));
		switch (resolution.getStatus()) {
			case FOUND -> {
				markQueryDeprecation(resolution);
				ObjectId userId = accessTokenService.validate(resolution.getToken(), isCountAsActivity(request));
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
			case CONFLICT, INVALID_BEARER, URL_TOKEN_REJECTED -> throw new BizException("NotLogin");
			case MISSING -> {
				// fall through to Basic
			}
		}

		if (request.getHeader("authorization") != null) {
			UserDetail userDetail = null;
			String authorization = request.getHeader("authorization").trim();
			if (authorization.contains(" ")) {
				String[] array = authorization.split(" ");
				String authorizationType = array.length > 0 ? array[0] : null;
				String authorizationParams = array.length > 1 ? array[1] : null;
				if ("Basic".equalsIgnoreCase(authorizationType) && null != authorizationParams) {
                    byte[] bytes = authorizationParams.getBytes(StandardCharsets.US_ASCII);
                    bytes = Base64.getDecoder().decode(bytes);
                    authorizationParams = new String(bytes, StandardCharsets.UTF_8);
					if (authorizationParams.contains(":")) {
						array = authorizationParams.split(":", 2);
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

	private void markQueryDeprecation(AccessTokenResolution resolution) {
		if (resolution.getSource() != AccessTokenSource.QUERY) {
			return;
		}
		try {
			ServletRequestAttributes attrs = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
			if (attrs == null) {
				return;
			}
			HttpServletResponse response = attrs.getResponse();
			if (response != null) {
				response.setHeader("Deprecation", "true");
				response.setHeader("X-Auth-Source", "query");
			}
		} catch (Exception e) {
			log.debug("Unable to set URL-token deprecation header: {}", e.getMessage());
		}
	}

	private boolean isCountAsActivity(HttpServletRequest request) {
		String header = request.getHeader(USER_ACTIVITY_HEADER);
		return !"0".equals(header);
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
