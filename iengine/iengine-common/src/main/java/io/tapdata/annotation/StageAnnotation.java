package io.tapdata.annotation;

import com.tapdata.entity.DatabaseTypeEnum;

import java.lang.annotation.*;

@Inherited
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(value = StageAnnotations.class)
public @interface StageAnnotation {

	/**
	 * 数据源类型
	 * @return
	 */
	DatabaseTypeEnum databaseType();
}
