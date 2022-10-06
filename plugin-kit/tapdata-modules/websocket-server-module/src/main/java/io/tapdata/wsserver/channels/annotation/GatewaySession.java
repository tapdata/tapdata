package io.tapdata.wsserver.channels.annotation;

import java.lang.annotation.*;

@Target(value = {ElementType.TYPE} )
@Retention(value = RetentionPolicy.RUNTIME)
@Documented
public @interface GatewaySession {
	String idType();
}