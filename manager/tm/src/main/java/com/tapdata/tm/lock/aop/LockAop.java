package com.tapdata.tm.lock.aop;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.lock.annotation.Lock;
import com.tapdata.tm.lock.service.LockService;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: Zed
 * @Date: 2021/12/17
 * @Description: 分布式的aop实现
 */
@Aspect
@Slf4j
@Component
public class LockAop {
	@Autowired
	private LockService lockService;


	/**
	 * @param pjp
	 * @return
	 * @throws Throwable
	 */
	@Around("(@annotation(com.tapdata.tm.lock.annotation.Lock)) && execution(* com.tapdata.tm.*.service..*.*(..))")
	public Object lock(ProceedingJoinPoint pjp) throws Throwable {
		MethodSignature methodSignature = (MethodSignature) pjp.getSignature();
		Lock annotation = methodSignature.getMethod().getAnnotation(Lock.class);
		//锁的key 一般为taskId等
		String value = annotation.value();
		String type = annotation.type().name();
		//锁设置的过期时间
		int expireSeconds = annotation.expireSeconds();
		//获取锁失败后的重试等待时间
		int sleepMillis = annotation.sleepMillis();

		if (StringUtils.isBlank(value)) {
			throw new BizException("SystemError");
		}

		List<String> values = Arrays.asList(value.split("\\."));
		String param = values.get(0);
		String [] parameters = methodSignature.getParameterNames();
		int paramIndex = -1;
		for (int i = 0; i < parameters.length; i++) {
			if (param.equals(parameters[i])) {
				paramIndex = i;
				break;
			}
		}
		if (paramIndex == -1) {
			throw new BizException("SystemError");
		}
		Object[] args = pjp.getArgs();
		Object obj = args[paramIndex];

		//如果是类似record.id的主机value值，需要一步一步的获取属性
		for (int i = 1; i < values.size(); i++) {
			Map<String, Object> objectMap = bean2Map(obj);
			obj = objectMap.get(values.get(i));
		}

		String key = obj instanceof String ? (String) obj : obj.toString();
		key = type + "_" + key;
		if (StringUtils.isBlank(key)) {
			throw new BizException("SystemError");
		}
		log.debug("pre get lock, key = {}, expireSeconds = {}, sleepMillis = {}",
				key, expireSeconds, sleepMillis);

		try {
			//todo 可能存在第一个锁获取到之后，后面的锁还没有获取到，就已经过期了
			boolean getlock = lockService.lock(key, expireSeconds, sleepMillis);
			if (!getlock) {
				log.debug("get lock failed, key = {}, expireSeconds = {}, sleepMillis = {}",
						key, expireSeconds, sleepMillis);
				throw new BizException("SystemError");
			}
			log.debug("get lock success");
			return pjp.proceed();
		} finally {
			lockService.release(key);
			log.debug("release lock success");
		}


	}

	/**
	 * 转换bean为map
	 *
	 * @param source 要转换的bean
	 * @param <T>    bean类型
	 * @return 转换结果
	 */
	public static <T> Map<String, Object> bean2Map(T source) throws IllegalAccessException {
		Map<String, Object> result = new HashMap<>();

		Class<?> sourceClass = source.getClass();
		//拿到所有的字段,不包括继承的字段
		Field[] sourceFiled = sourceClass.getDeclaredFields();
		for (Field field : sourceFiled) {
			field.setAccessible(true);
			result.put(field.getName(), field.get(source));
		}
		return result;
	}
}