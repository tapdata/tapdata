package io.tapdata.task;

import com.tapdata.entity.DatabaseTypeEnum;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TaskType {

	String type();
}
