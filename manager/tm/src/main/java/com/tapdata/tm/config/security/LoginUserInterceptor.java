package com.tapdata.tm.config.security;

import com.tapdata.tm.base.annotation.IgnoreLogin;
import com.tapdata.tm.base.security.LoginUserResolver;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.stereotype.Component;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;

@Component
public class LoginUserInterceptor implements HandlerInterceptor {

	private final LoginUserResolver loginUserResolver;

	public LoginUserInterceptor(LoginUserResolver loginUserResolver) {
		this.loginUserResolver = loginUserResolver;
	}

	@Override
	public boolean preHandle(@NotNull HttpServletRequest request
        , @NotNull HttpServletResponse response
        , @NotNull Object handler
    ) {
		if (handler instanceof HandlerMethod handlerMethod) {
            if (isIgnoreLogin(handlerMethod)) {
                return true;
            }

            loginUserResolver.resolve(request);
		}
		return true;
	}

	private boolean isIgnoreLogin(HandlerMethod handlerMethod) {
		return AnnotatedElementUtils.hasAnnotation(handlerMethod.getMethod(), IgnoreLogin.class)
				|| AnnotatedElementUtils.hasAnnotation(handlerMethod.getBeanType(), IgnoreLogin.class);
	}
}
