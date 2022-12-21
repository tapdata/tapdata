package io.tapdata.exception;

import java.lang.annotation.*;

/**
 * Use on static filed with exception code
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/13 15:12 Create
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TapExCode {
}
