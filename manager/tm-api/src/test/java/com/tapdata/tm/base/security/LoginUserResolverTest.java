package com.tapdata.tm.base.security;

import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.component.ProductComponent;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.service.UserService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LoginUserResolverTest {

	private LoginUserResolver loginUserResolver;
	private UserService userService;
	private AccessTokenService accessTokenService;
	private ProductComponent productComponent;

	@BeforeEach
	void setUp() {
		loginUserResolver = new LoginUserResolver();
		userService = mock(UserService.class);
		accessTokenService = mock(AccessTokenService.class);
		productComponent = mock(ProductComponent.class);
		ReflectionTestUtils.setField(loginUserResolver, "userService", userService);
		ReflectionTestUtils.setField(loginUserResolver, "accessTokenService", accessTokenService);
		ReflectionTestUtils.setField(loginUserResolver, "productComponent", productComponent);
	}

	@Nested
	class ResolveTest {

		@Test
		void testReturnCachedUserWhenNoSpecifiedUserId() {
			MockHttpServletRequest request = request("GET", "/api/Connections");
			UserDetail cachedUser = user("cached");
			request.setAttribute(LoginUserResolver.LOGIN_USER_ATTRIBUTE, cachedUser);

			UserDetail actual = loginUserResolver.resolve(request);

			assertSame(cachedUser, actual);
			verify(userService, never()).loadUserByExternalId(anyString());
		}

		@Test
		void testResolveBySpecifiedUserIdWithoutUsingCache() {
			MockHttpServletRequest request = request("GET", "/api/Connections");
			UserDetail cachedUser = user("cached");
			UserDetail specifiedUser = user("specified");
			request.setAttribute(LoginUserResolver.LOGIN_USER_ATTRIBUTE, cachedUser);
			when(userService.loadUserByExternalId("external-user")).thenReturn(specifiedUser);

			UserDetail actual = loginUserResolver.resolve(request, "external-user");

			assertSame(specifiedUser, actual);
			assertSame(cachedUser, request.getAttribute(LoginUserResolver.LOGIN_USER_ATTRIBUTE));
		}

		@Test
		void testResolveByUserIdHeaderAndCacheResult() {
			MockHttpServletRequest request = request("GET", "/api/Connections");
			request.addHeader("user_id", "external-user");
			UserDetail userDetail = user("header");
			when(userService.loadUserByExternalId("external-user")).thenReturn(userDetail);

			UserDetail actual = loginUserResolver.resolve(request);

			assertSame(userDetail, actual);
			assertSame(userDetail, request.getAttribute(LoginUserResolver.LOGIN_USER_ATTRIBUTE));
		}

		@Test
		void testResolveByAccessToken() {
			ObjectId userId = new ObjectId();
			MockHttpServletRequest request = request("GET", "/api/Connections");
			request.setQueryString("name=test&access_token=token%2Bvalue");
			UserDetail userDetail = user("access-token");
			when(accessTokenService.validate("token+value", true)).thenReturn(userId);
			when(userService.loadUserById(userId)).thenReturn(userDetail);

			UserDetail actual = loginUserResolver.resolve(request);

			assertSame(userDetail, actual);
		}

		@Test
		void testResolveByBasicAuthorization() {
			MockHttpServletRequest request = request("GET", "/api/Connections");
			String credential = Base64.getEncoder()
					.encodeToString("admin:Gotapd8!".getBytes(StandardCharsets.UTF_8));
			request.addHeader("authorization", "Basic " + credential);
			UserDetail userDetail = user("basic");
			when(userService.loadUserByUsername("admin")).thenReturn(userDetail);

			UserDetail actual = loginUserResolver.resolve(request);

			assertSame(userDetail, actual);
		}

		@Test
		void testSetFreeAuthWhenDaaSAndUriMatched() {
			MockHttpServletRequest request = request("GET", "/api/Workers/worker-id");
			request.addHeader("user_id", "external-user");
			UserDetail userDetail = user("free-auth");
			when(productComponent.isDAAS()).thenReturn(true);
			when(userService.loadUserByExternalId("external-user")).thenReturn(userDetail);

			UserDetail actual = loginUserResolver.resolve(request);

			assertTrue(actual.isFreeAuth());
		}

		@Test
		void testDoNotSetFreeAuthWhenDaaSAndUriExcluded() {
			MockHttpServletRequest request = request("GET", "/api/clusterStates/findAccessNodeInfo");
			request.addHeader("user_id", "external-user");
			UserDetail userDetail = user("not-free-auth");
			when(productComponent.isDAAS()).thenReturn(true);
			when(userService.loadUserByExternalId("external-user")).thenReturn(userDetail);

			UserDetail actual = loginUserResolver.resolve(request);

			assertFalse(actual.isFreeAuth());
		}

		@Test
		void testThrowNotLoginWhenAccessTokenInvalid() {
			MockHttpServletRequest request = request("GET", "/api/Connections");
			request.setQueryString("access_token=invalid");
			when(accessTokenService.validate("invalid", true)).thenReturn(null);

			assertThrows(BizException.class, () -> loginUserResolver.resolve(request));
		}

		@Test
		void testResolveByAccessTokenWithPassiveHeader() {
			// X-User-Activity: 0 表示被动请求（如前端定时轮询），不应计入用户活跃
			ObjectId userId = new ObjectId();
			MockHttpServletRequest request = request("GET", "/api/measurement/batch");
			request.setQueryString("access_token=t1");
			request.addHeader(LoginUserResolver.USER_ACTIVITY_HEADER, "0");
			UserDetail userDetail = user("polling");
			when(accessTokenService.validate("t1", false)).thenReturn(userId);
			when(userService.loadUserById(userId)).thenReturn(userDetail);

			UserDetail actual = loginUserResolver.resolve(request);

			assertSame(userDetail, actual);
			verify(accessTokenService, times(1)).validate("t1", false);
			verify(accessTokenService, never()).validate("t1", true);
		}

		@Test
		void testResolveByAccessTokenWithUnknownActivityHeaderTreatedAsActive() {
			// 仅当 header 显式为 "0" 时才算被动，其他取值（异常或拼写错误）一律按活跃处理，保持向后兼容
			ObjectId userId = new ObjectId();
			MockHttpServletRequest request = request("GET", "/api/Connections");
			request.setQueryString("access_token=t2");
			request.addHeader(LoginUserResolver.USER_ACTIVITY_HEADER, "passive");
			UserDetail userDetail = user("active-fallback");
			when(accessTokenService.validate("t2", true)).thenReturn(userId);
			when(userService.loadUserById(userId)).thenReturn(userDetail);

			UserDetail actual = loginUserResolver.resolve(request);

			assertSame(userDetail, actual);
			verify(accessTokenService, times(1)).validate("t2", true);
		}

		@Test
		void testThrowWrongPasswordWhenBasicPasswordInvalid() {
			MockHttpServletRequest request = request("GET", "/api/Connections");
			String credential = Base64.getEncoder()
					.encodeToString("admin:wrong".getBytes(StandardCharsets.UTF_8));
			request.addHeader("authorization", "Basic " + credential);

			assertThrows(BizException.class, () -> loginUserResolver.resolve(request));
			verify(userService, never()).loadUserByUsername("admin");
		}
	}

	@Nested
	class IsFreeAuthTest {

		@Test
		void testMatchWhiteList() {
			assertTrue(LoginUserResolver.isFreeAuth("/api/customNode/123", "get"));
		}

		@Test
		void testExcludeWithoutList() {
			assertFalse(LoginUserResolver.isFreeAuth("/api/clusterStates/findAccessNodeInfo", "GET"));
		}

		@Test
		void testReturnFalseWhenMethodNotConfigured() {
			assertFalse(LoginUserResolver.isFreeAuth("/api/Workers/123", "POST"));
		}
	}

	private MockHttpServletRequest request(String method, String uri) {
		MockHttpServletRequest request = new MockHttpServletRequest(method, uri);
		request.setRequestURI(uri);
		return request;
	}

	private UserDetail user(String username) {
		return new UserDetail("user-id", "customer-id", username, "password", Collections.emptyList());
	}
}
