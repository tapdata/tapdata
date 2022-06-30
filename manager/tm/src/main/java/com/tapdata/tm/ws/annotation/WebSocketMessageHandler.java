/**
 * @title: WebSocketMessageHandler
 * @description:
 * @author lk
 * @date 2021/9/9
 */
package com.tapdata.tm.ws.annotation;

import com.tapdata.tm.ws.enums.MessageType;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Inherited
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface WebSocketMessageHandler {

	MessageType[] type();
}
