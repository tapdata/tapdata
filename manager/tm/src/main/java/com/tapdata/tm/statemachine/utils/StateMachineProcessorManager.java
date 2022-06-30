/**
 * @title: JobStatusUtils
 * @description:
 * @author lk
 * @date 2021/7/19
 */
package com.tapdata.tm.statemachine.utils;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.statemachine.Processor;
import com.tapdata.tm.statemachine.StateMachine;
import com.tapdata.tm.statemachine.annotation.OnAction;
import com.tapdata.tm.statemachine.annotation.StateMachineHandler;
import com.tapdata.tm.statemachine.annotation.StateMachineHandlers;
import com.tapdata.tm.statemachine.annotation.WithStateMachine;
import com.tapdata.tm.statemachine.enums.Transitions;
import com.tapdata.tm.statemachine.model.StateContext;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.model.Transition;
import com.tapdata.tm.statemachine.processor.DefaultStateMachineProcessor;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import java.io.File;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.ReflectionUtils;

@Slf4j
public class StateMachineProcessorManager {

	private static final Map<String, Processor> stateMachineProcessorHashMap = new HashMap<>();

	private static final Map<String, MethodInvokerInfo> methodInvokerHelperCache = new ConcurrentHashMap<>();

	private static final String CLASS_SUFFIX = ".class";
	public static final String PACKAGE_NAME = "com/tapdata/tm/statemachine/processor";

	public static Processor getProcessor(Object state, Object event) {
		return getProcessor(getKey(state, event));
	}

	public static Processor getProcessor(String key) {
		return stateMachineProcessorHashMap.get(key);
	}

	public static Processor getDefaultProcessor() {
		return SpringContextHelper.getBean(DefaultStateMachineProcessor.class);
	}

	public static void init() throws Exception {
		buildstateMachineProcessor();
		initMethodInvokerHelperCache();
	}

	private static void buildstateMachineProcessor() throws Exception {
		Set<Class<?>> set = new HashSet<>();
		Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources(PACKAGE_NAME);
		while (urls != null && urls.hasMoreElements()){
			URL url = urls.nextElement();
			String protocol = url.getProtocol();
			log.info("Urls path: {}, protocol: {}", url.getPath(), url.getProtocol());
			if ("file".equals(protocol)) {
				String file = URLDecoder.decode(url.getFile(), "UTF-8");
				File dir = new File(file);
				if (dir.isDirectory()) {
					parseClassFile(dir, set);
				}
			}else if ("jar".equals(protocol)) {
				JarFile jar = ((JarURLConnection) url.openConnection()).getJarFile();
				Enumeration<JarEntry> entries = jar.entries();
				while (entries.hasMoreElements()) {
					JarEntry entry = entries.nextElement();
					if (entry.isDirectory()) {
						continue;
					}
					String name = entry.getName();
					if (name.endsWith(CLASS_SUFFIX)) {
						set.add(Class.forName(name.substring(0, name.length() - CLASS_SUFFIX.length()).replaceAll("/", ".")));
					}
				}
			}
		}

		if (CollectionUtils.isNotEmpty(set)){
			for (Class<?> aClass : set) {
				if (!aClass.isInterface() && aClass.isAnnotationPresent(StateMachineHandler.class)) {
					StateMachineHandler stateMachineHandler = aClass.getDeclaredAnnotation(StateMachineHandler.class);
					Object o = SpringContextHelper.getBean(aClass);
					if (o instanceof Processor){
						Transitions transitions = stateMachineHandler.transitions();
						Arrays.stream(transitions.getSources()).forEach(state -> stateMachineProcessorHashMap.put(getKey(state, transitions.getEvent()), (Processor) o));
					}
				}else if (!aClass.isInterface() && aClass.isAnnotationPresent(StateMachineHandlers.class)) {
					StateMachineHandlers stateMachineHandlers = aClass.getDeclaredAnnotation(StateMachineHandlers.class);
					Object o = SpringContextHelper.getBean(aClass);
					if (o instanceof Processor){
						Arrays.stream(stateMachineHandlers.value())
								.map(StateMachineHandler::transitions)
								.forEach(transitions -> Arrays.stream(transitions.getSources())
										.forEach(state -> stateMachineProcessorHashMap.put(getKey(state, transitions.getEvent()), (Processor) o)));
					}
				}
			}
		}else {
			log.warn("GetProcessor warn, set is empty");
		}
	}



	private static void parseClassFile(File dir, Set<Class<?>> set) throws ClassNotFoundException {
//		log.info("Parse class file start, dir.name: {}", dir.getName());
		if (dir.isDirectory()) {
			File[] files = dir.listFiles();
			if (files == null){
				log.warn("parse file failed, files is null");
				return;
			}
			for (File file : files) {
				parseClassFile(file, set);
			}
		} else if (dir.getName().endsWith(CLASS_SUFFIX)) {
			String name = dir.getPath();
			name = name.substring(name.indexOf("classes") + 8).replace("\\", ".");

			set.add(Class.forName(name.substring(0, name.length() - CLASS_SUFFIX.length()).replaceAll("/", ".")));
		}
	}


	private static void initMethodInvokerHelperCache() {
		Map<String, Object> beans = SpringContextHelper.getBeansWithAnnotation(WithStateMachine.class);
		if (MapUtils.isNotEmpty(beans)){
			beans.values().forEach(StateMachineProcessorManager::addMethodInvokerHelper);
		}
	}

	private static void addMethodInvokerHelper(Object bean){
		Class<?> targetClass = AopUtils.getTargetClass(bean);
		ReflectionUtils.doWithMethods(targetClass, method -> {

			if (AnnotatedElementUtils.isAnnotated(method, OnAction.class)){
				OnAction onAction = AnnotationUtils.getAnnotation(method, OnAction.class);
				if (onAction != null){
					Transitions[] transitions = onAction.transitions();
					Arrays.stream(transitions)
							.forEach(transition -> Arrays.stream(transition.getSources())
									.forEach(state -> addMethodInvokerHelper(bean, getKey(state, transition.getEvent()), method)));
				}

			}
		});
	}

	private static String getKey(Object state, Object event){
		return String.format("%s_%s_%s_%s", state.getClass().getName(), state, event.getClass().getName(), event);
	}

	private static void addMethodInvokerHelper(Object bean, String key, Method method){
		methodInvokerHelperCache.put(key, new MethodInvokerInfo(bean, method));
	}

	public static <S, E> Function<StateContext<S, E>, StateMachineResult> getAction(Transition<S, E> transition, Class<? extends StateContext<S, E>> contextClass, StateMachine<S, E> stateMachine){
		MethodInvokerInfo methodInvokerInfo = getMethodInvokerHelper(getKey(transition.getSource(), transition.getEvent()));
		if (methodInvokerInfo == null || methodInvokerInfo.getMethod() == null){
			return null;
		}
		return methodInvokerInfo.buildAction(contextClass, stateMachine);
	}


	private static MethodInvokerInfo getMethodInvokerHelper(String key){
		return methodInvokerHelperCache.get(key);
	}

	static class MethodInvokerInfo {
		private Object bean;

		private Method method;

		private StandardEvaluationContext evaluationContext;

		private Function<Class<? extends StateContext>, Expression> expressionFunc;

		private Class<?> expectedType;

		private static final SpelExpressionParser EXPRESSION_PARSER = new SpelExpressionParser();

		MethodInvokerInfo(Object bean, Method method) {
			this.bean = bean;
			this.method = method;
			this.expectedType = method.getReturnType() == Void.TYPE ? null : method.getReturnType();
			this.evaluationContext = new StandardEvaluationContext();
			this.expressionFunc = generateExpression();
			prepareEvaluationContext();
		}

		private void prepareEvaluationContext() {
			Class<?> targetType = AopUtils.getTargetClass(bean);
			evaluationContext.registerMethodFilter(targetType, methods -> methods.contains(this.method) ? Collections.singletonList(method) : Collections.emptyList());
			evaluationContext.setVariable("context", bean);;
			Method declaredMethod = null;
			try {
				declaredMethod = ParametersWrapper.class.getDeclaredMethod("checkClass", Object.class, Class.class);
			}catch (Exception e){
				log.error("ParametersWrapper getDeclaredMethod error,message: {}", e.getMessage(), e);
			}
			if (declaredMethod != null){
				evaluationContext.registerFunction("getData", declaredMethod);
			}
		}

		public Object getBean() {
			return bean;
		}

		public Method getMethod() {
			return method;
		}

		private Function<Class<? extends StateContext>, Expression> generateExpression() {
			return (contextClass) -> {
				StringBuilder sb = new StringBuilder("#context." + method.getName() + "(");
				Class<?>[] parameterTypes = method.getParameterTypes();
				List<? extends Class<? extends Serializable>> classes = Arrays.asList(Byte.TYPE, Character.TYPE, Double.TYPE, Float.TYPE, Integer.TYPE, Long.TYPE, Short.TYPE);
				IntStream.range(0, parameterTypes.length).forEach(i -> {
					if (i != 0) {
						sb.append(", ");
					}
					MethodParameter methodParameter = new MethodParameter(method, i);
					TypeDescriptor parameterTypeDescriptor = new TypeDescriptor(methodParameter);
					Class<?> parameterType = parameterTypeDescriptor.getObjectType();
					if (parameterType == StateContext.class || contextClass.isAssignableFrom(parameterType)) {
						sb.append("stateContext");
					} else if (StateMachine.class.isAssignableFrom(parameterType)) {
						Type[] types = ((ParameterizedType) contextClass.getGenericSuperclass()).getActualTypeArguments();
						Class<?>[] resolveGenerics = parameterTypeDescriptor.getResolvableType().resolveGenerics();
						if (resolveGenerics.length != types.length){
							sb.append("null");
						}else {
							boolean b = IntStream.range(0, resolveGenerics.length)/*.filter(j -> resolveGenerics[j] != null)*/.noneMatch(j -> resolveGenerics[j] != types[j]);
							sb.append(b ? "stateMachine" : "null");
						}
					} else if (StateMachineService.class.isAssignableFrom(parameterType)) {
						sb.append("stateMachineService");
					} else if (BaseDto.class.isAssignableFrom(parameterType)) {
						sb.append("(#getData(data,T(").append(parameterType.getName()).append("))?data:null)");
					} else if (UserDetail.class == parameterType) {
						sb.append("stateContext?.userDetail");
					} else {
						if (parameterTypes[i].isPrimitive()) {
							if (parameterTypes[i] == Boolean.TYPE) {
								sb.append("false");
							} else if (classes.contains(parameterTypes[i])) {
								sb.append("0");
							}
						} else {
							sb.append("null");
						}
					}
				});
				sb.append(")");
				return EXPRESSION_PARSER.parseExpression(sb.toString());
			};
		}

		@Deprecated
		<S, E> Function<StateContext<S, E>, StateMachineResult> buildAction(Class<? extends StateContext<S, E>> contextClass){
			if (method.getParameterTypes().length != 0 && contextClass != null && !Arrays.asList(method.getParameterTypes()).contains(contextClass)){
				return null;
			}
			if (!StateMachineResult.class.isAssignableFrom(method.getReturnType())){
				return null;
			}
			return context -> {
				try {
					Class<?>[] parameterTypes = method.getParameterTypes();
					if (parameterTypes.length == 1 && StateContext.class.isAssignableFrom(parameterTypes[0])){
						Object args = null;
						if (Arrays.stream(parameterTypes[0].getConstructors()).anyMatch(constructor -> constructor.getParameterCount() == 0)) {
							args = parameterTypes[0].newInstance();
							BeanUtils.copyProperties(context, args);
						}

						return (StateMachineResult) method.invoke(bean, args);
					}else {
						return (StateMachineResult) method.invoke(bean);
					}
				} catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
					log.error("Method invoke failed,methodName: {},message: {}", method.getName(), e.getMessage(), e);
					if (e instanceof InvocationTargetException){
						if (((InvocationTargetException) e).getTargetException() instanceof BizException){
							BizException bizException = (BizException)((InvocationTargetException) e).getTargetException();
							throw new BizException(bizException.getErrorCode(), bizException.getArgs());
						}
						throw new BizException(((InvocationTargetException) e).getTargetException());
					}
					throw new BizException(e);
				}
			};
		}

		<S, E> Function<StateContext<S, E>, StateMachineResult> buildAction(Class<? extends StateContext<S, E>> contextClass, StateMachine<S, E> stateMachine){
			Expression expression = expressionFunc.apply(contextClass);
			return context -> {
				ParametersWrapper<S, E> parametersWrapper = new ParametersWrapper<>(context, stateMachine);
				Object result = expression.getValue(evaluationContext, parametersWrapper, expectedType);
				if (result instanceof StateMachineResult){
					return (StateMachineResult) result;
				}else {
					return StateMachineResult.ok();
				}
			};
		}
	}

	public static class ParametersWrapper<S, E> {

		private StateContext<S, E> stateContext;

		private StateMachine<S, E> stateMachine;

		ParametersWrapper(StateContext<S, E> stateContext, StateMachine<S, E> stateMachine) {
			this.stateContext = stateContext;
			this.stateMachine = stateMachine;
		}

		static boolean checkClass(Object data, Class<?> dataClass){
			return data != null && data.getClass() == dataClass;
		}

		public StateContext<S, E> getStateContext() {
			return stateContext;
		}

		public StateMachine<S, E> getStateMachine() {
			return stateMachine;
		}

		public StateMachineService getStateMachineService() {
			try {
				return SpringContextHelper.getBean(StateMachineService.class);
			}catch (Exception e){
				log.error("Failed to get statemachineservice,message: {}", e.getMessage(), e);
				return null;
			}
		}

		public Object getData(){
			return stateContext.getData();
		}
	}

}
