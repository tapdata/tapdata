package com.tapdata.tm.commons.websocket;

import java.lang.annotation.*;

/**
 * allow method to remote call
 *
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/14 下午4:18
 */
@Documented
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface AllowRemoteCall {
}
