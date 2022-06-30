package com.tapdata.tm.base.validation.validator;

import com.tapdata.tm.base.validation.constraints.CustomerTypeSubset;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/13 6:18 下午
 * @description
 */
public class CustomerTypeSubsetValidator implements ConstraintValidator<CustomerTypeSubset, String> {

	private List<String> subset = Collections.emptyList();

	@Override
	public void initialize(CustomerTypeSubset constraintAnnotation) {
		this.subset = Arrays.asList(constraintAnnotation.anyOf());
	}

	@Override
	public boolean isValid(String value, ConstraintValidatorContext context) {
		return value == null || subset.contains(value);
	}
}
