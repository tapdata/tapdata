package com.tapdata.tm.base.handler;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.task.dto.Message;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.commons.util.ThrowableUtils;
import com.tapdata.tm.utils.WebUtils;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.http.HttpStatus;
import org.springframework.core.annotation.Order;
import org.springframework.data.util.Streamable;
import org.springframework.validation.BindException;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/13 2:19 下午
 * @description
 */
@RestControllerAdvice
@Order(-2)
@Slf4j
public class ExceptionHandler extends BaseController {

	@org.springframework.web.bind.annotation.ExceptionHandler(Throwable.class)
	public ResponseMessage<?> handlerException(Throwable e, HttpServletRequest request, HttpServletResponse response) throws Throwable {

		Locale locale = WebUtils.getLocale(request);

		String errorCode = "SystemError";
		if (e instanceof NoResourceFoundException) {
			if (e.getMessage() != null && e.getMessage().contains("actuator/prometheus")) {
				log.warn("No static resource actuator/prometheus");
				return failed(errorCode, "No static resource actuator/prometheus");
			} else {
				log.error("System error", e);
				throw e;
			}
		}
		log.error("System error", e);
		String message = e.getMessage();

		if (e instanceof BizException){
			return handlerException((BizException) e, request, response);
		} else if (isDqlRequest(request) && isNoPermission(e)) {
			errorCode = "NoPermission";
			message = null;
		} else if (e instanceof IllegalArgumentException) {
			errorCode = "IllegalArgument";
			message = e.getMessage();
		} else if (e instanceof IllegalStateException) {
			errorCode = "IllegalState";
			message = e.getMessage();
		} else if (e instanceof MethodArgumentNotValidException) {
			errorCode = "IllegalArgument";
			MethodArgumentNotValidException error = (MethodArgumentNotValidException) e;
			message = collectValidResult(locale, error.getBindingResult(), error);
		} else if (e instanceof BindException) {
			errorCode = "IllegalArgument";
			BindException error = (BindException) e;
			message = collectValidResult(locale, error.getBindingResult(), error);
		}

		if ("NoPermission".equals(errorCode)) {
			message = null;
		}
		String msg = "NoPermission".equals(errorCode)
			? MessageUtil.getMessage(locale, errorCode)
			: MessageUtil.getMessage(locale, errorCode, message);
		applyHttpStatus(errorCode, request, response);

		return failed(errorCode, msg);
	}

	private String collectValidResult(Locale locale, BindingResult result , Exception e) {
		try {
			return result.getAllErrors().stream().map(new Function<ObjectError, Object>() {
				@Override
				public Object apply(ObjectError objectError) {

					if (objectError instanceof FieldError) {
						FieldError fieldError = (FieldError) objectError;

						String defaultMessage = fieldError.getDefaultMessage();
						String fieldPath = fieldError.getField();
						Object _value = fieldError.getRejectedValue();
						String value = null;
						if (_value != null)
							value = _value.toString();
						//((ConstraintViolationImpl)((ViolationFieldError)fieldError).violation).propertyPath

						if (defaultMessage != null &&
							defaultMessage.startsWith("{") &&
							defaultMessage.endsWith("}")) {
							defaultMessage = MessageUtil.getMessage(locale, defaultMessage.substring(1, defaultMessage.length() - 1));
						}

						if (defaultMessage != null && defaultMessage.contains("{") && defaultMessage.contains("}")) {
							Map<String, Object> map = new HashMap<>();
							map.put("field", fieldPath);
							map.put("value", value);
							if (fieldError.getArguments() != null && fieldError.getArguments().length >= 2) {
								Class clazz = ((Class) fieldError.getArguments()[1]);
								if (clazz.isEnum()) {
									Object[] enums = clazz.getEnumConstants();
									String enumList = Streamable.of(enums).stream().map(Object::toString).collect(Collectors.joining(","));
									map.put("enums", enumList);
									map.put("enumClass", clazz.getSimpleName());
								}
							}

							defaultMessage = MessageUtil.formatString(defaultMessage, map);
						}

						return fieldPath + ": " + defaultMessage;
					}

					return objectError.getObjectName() + ", " + objectError.getDefaultMessage();
				}
			}).map(Object::toString).collect(Collectors.joining("; "));
		} catch (Throwable e1) {
			log.error("collect error message failed", e1);
		}
		return e.getMessage();
	}

	@org.springframework.web.bind.annotation.ExceptionHandler(BizException.class)
	public ResponseMessage<?> handlerException(BizException e, HttpServletRequest request, HttpServletResponse response) {
		log.error("System error:{}", ThrowableUtils.getStackTraceByPn(e));
		if ("NotAuthorized".equals(e.getErrorCode()) || "NotLogin".equals(e.getErrorCode())) {
			response.setStatus(HttpStatus.SC_UNAUTHORIZED);
		}
		applyHttpStatus(e.getErrorCode(), request, response);

		//对于一些需要返回多个节点的多个错误信息的约定返回异常码
		if ("Task.ListWarnMessage".equals(e.getErrorCode())) {
			return listWarnMessage(e, request);
		}


		if ("Ldp.MdmTargetNoPrimaryKey".equals(e.getErrorCode())) {
			String message = MessageUtil.getMessage(WebUtils.getLocale(request), e.getErrorCode());
			ResponseMessage<Object> failed = failed(e.getErrorCode(), message);
			Object[] args = e.getArgs();
			if (args != null && args.length != 0) {
				failed.setData(args[0]);
			}
			return failed;
		}

		String message = MessageUtil.getMessage(WebUtils.getLocale(request), e.getErrorCode(), e.getArgs());


		ResponseMessage<Object> failed;
		if ("NotLogin".equals(e.getErrorCode())) {
			failed = failed(e.getErrorCode(), message);
		} else if (!StringUtils.isEmpty(message)) {
			failed = failed(e.getErrorCode(), message, e);
		} else {

			failed = failed(e.getErrorCode(), e);

		}

		return failed;
	}

	private void applyHttpStatus(String errorCode, HttpServletRequest request, HttpServletResponse response) {
		if (response == null) {
			return;
		}
		int status = httpStatusFor(errorCode, request);
		if (status > 0) {
			response.setStatus(status);
		}
	}

	private int httpStatusFor(String errorCode, HttpServletRequest request) {
		boolean dqlRequest = isDqlRequest(request);
		if ("NoPermission".equals(errorCode)) {
			return dqlRequest ? HttpStatus.SC_FORBIDDEN : 0;
		}
		if (!dqlRequest) {
			return 0;
		}
		return switch (errorCode) {
			case "IllegalArgument", "DqlEvent.InvalidPayload", "DqlEvent.InvalidRouteDecision",
				"DqlRecovery.CrossTaskNotAllowed" -> HttpStatus.SC_BAD_REQUEST;
			case "Task.NotFound", "DqlEvent.NotFound", "DqlRecovery.BatchNotFound" -> HttpStatus.SC_NOT_FOUND;
			case "DqlRecovery.EventNotReprocessable", "DqlRecovery.EventLockFailed" -> HttpStatus.SC_CONFLICT;
			case "SystemError" -> HttpStatus.SC_INTERNAL_SERVER_ERROR;
			default -> 0;
		};
	}

	private boolean isDqlRequest(HttpServletRequest request) {
		if (request == null) {
			return false;
		}
		String requestUri = request.getRequestURI();
		return requestUri != null && requestUri.contains("/dql-events");
	}

	private boolean isNoPermission(Throwable e) {
		return e != null && "NoPermission".equals(e.getMessage());
	}

	public ResponseMessage<Map<String, List<Message>>> listWarnMessage(BizException e, HttpServletRequest request) {
		Object[] args = e.getArgs();
		Map<String, List<Message>> messageMap = (Map<String, List<Message>>) args[0];
		for (Map.Entry<String, List<Message>> entry : messageMap.entrySet()) {
			List<Message> value = entry.getValue();
			if (CollectionUtils.isNotEmpty(value)) {
				for (Message message : value) {
					String msg = MessageUtil.getMessage(WebUtils.getLocale(request), message.getCode());
					message.setMsg(msg);
				}
			}
		}

		String msg = MessageUtil.getMessage(WebUtils.getLocale(request), e.getErrorCode());
		ResponseMessage<Map<String, List<Message>>> res = new ResponseMessage<>();
		res.setCode(e.getErrorCode());
		res.setMessage(msg);
		res.setData(messageMap);
		return res;
	}
}
