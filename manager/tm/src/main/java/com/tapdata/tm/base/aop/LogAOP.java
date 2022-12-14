package com.tapdata.tm.base.aop;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.tapdata.tm.commons.util.ThrowableUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.*;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpSession;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/14 9:13 上午
 * @description
 */
@Aspect
@Component
public class LogAOP {

	private final Map<String, Logger> loggerCache = new HashMap<>();

	@Pointcut("execution(public * com.tapdata.tm.*.controller.*.*(..))")
	private void restApiLog(){}

	private Logger getLogger(Class clazz) {

		String className = clazz.getName();
		if (loggerCache.containsKey(className))
			return loggerCache.get(className);

		Logger logger = LoggerFactory.getLogger(clazz);
		loggerCache.put(className, logger);

		return logger;
	}

	@Before("restApiLog()")
	public void before(JoinPoint joinPoint){

		Logger logger = getLogger(joinPoint.getTarget().getClass());
		Signature signature = joinPoint.getSignature();
		if (signature instanceof MethodSignature) {
			Map<String, Object> params = new HashMap<>();

			MethodSignature methodSignature = (MethodSignature) signature;
			String[] parameterNames = methodSignature.getParameterNames();
			//Class[] parameterTypes = methodSignature.getParameterTypes();
			Object[] args = joinPoint.getArgs();

			for (int i = 0; i < args.length && i < parameterNames.length; i++) {
				Object obj = args[i];
				if( !(obj instanceof ServletRequest || obj instanceof ServletResponse || obj instanceof HttpSession) ){
					params.put(parameterNames[i], obj);
				}
			}

			if (logger.isDebugEnabled()) {
				logger.debug("{}, params:{}", joinPoint.getSignature().getName(),
						new Gson().toJson(params));
			}
		} else {
			Object[] args = joinPoint.getArgs();
			List<Object> params = new ArrayList<>();

			for (int i = 0; i < args.length; i++) {
				Object obj = args[i];
				if( !(obj instanceof ServletRequest || obj instanceof ServletResponse || obj instanceof HttpSession) ){
					params.add(obj);
				}
			}
			if (logger.isDebugEnabled()) {
				logger.debug("{}, params:{}", joinPoint.getSignature().getName(),
						new Gson().toJson(params));
			}
		}
	}
	/*@After("restApiLog()")
	public void after(JoinPoint joinPoint){
		logger.debug("after:" + joinPoint);
	}*/

	@AfterThrowing( throwing = "e", pointcut = "restApiLog()")
	public void error(JoinPoint joinPoint, Throwable e){
		Logger logger = getLogger(joinPoint.getTarget().getClass());
		logger.error(joinPoint.getSignature().getName() + ", error", ThrowableUtils.getStackTraceByPn(e));
	}

	@AfterReturning(returning = "result", pointcut = "restApiLog()")
	public void afterReturn(JoinPoint joinPoint, Object result){
		Logger logger = getLogger(joinPoint.getTarget().getClass());
		if (logger.isDebugEnabled()) {
			try {
				logger.debug("{}, result:{}",
					//这里改成Jackson的避免数据模型定义都是Jackson的转义注解，这边用gson会转义失败，甚至报错
					joinPoint.getSignature().getName(),
						new ObjectMapper().writeValueAsString(result));
			} catch (JsonProcessingException e) {
				logger.info("object to json failed");
			}
		}
	}

}
