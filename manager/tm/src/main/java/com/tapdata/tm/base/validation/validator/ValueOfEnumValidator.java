package com.tapdata.tm.base.validation.validator;

import com.tapdata.tm.base.validation.constraints.ValueOfEnum;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/13 6:28 下午
 * @description
 */
public class ValueOfEnumValidator implements ConstraintValidator<ValueOfEnum, String> {
	private List<String> acceptedValues = new ArrayList<>();

	@Override
	public void initialize(ValueOfEnum constraintAnnotation) {
		acceptedValues = Stream.of(constraintAnnotation.enumClass().getEnumConstants())
			.map(Enum::name)
			.collect(Collectors.toList());
	}

	@Override
	public boolean isValid(String value, ConstraintValidatorContext context) {
		if (value == null)
			return true;
		return acceptedValues.contains(value);
	}
}
