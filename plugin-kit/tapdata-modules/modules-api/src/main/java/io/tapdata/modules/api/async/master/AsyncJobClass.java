package io.tapdata.modules.api.async.master;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * @author aplomb
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface AsyncJobClass {
	String value();
}
