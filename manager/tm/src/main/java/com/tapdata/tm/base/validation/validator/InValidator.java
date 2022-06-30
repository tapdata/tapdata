package com.tapdata.tm.base.validation.validator;

import com.tapdata.tm.base.validation.constraints.In;

import java.util.stream.Collectors;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/13 6:28 下午
 * @description
 */
public class InValidator implements ConstraintValidator<In, Object> {
	private List<String> acceptedStrings = new ArrayList<>();
	private List<Integer> acceptedInteger = null;

	@Override
	public void initialize(In constraintAnnotation) {
		acceptedStrings = Arrays.asList(constraintAnnotation.strings());
		acceptedInteger = Arrays.stream(constraintAnnotation.numbers()).boxed().collect(Collectors.toList());
	}

	@Override
	public boolean isValid(Object value, ConstraintValidatorContext context) {
		if (value == null)
			return true;
		if (CollectionUtils.isNotEmpty(acceptedStrings)){
			return acceptedStrings.contains(value.toString());
		}else if (CollectionUtils.isNotEmpty(acceptedInteger)){
			return acceptedInteger.contains(Integer.valueOf(value.toString()));
		}
		return true;
	}
}
