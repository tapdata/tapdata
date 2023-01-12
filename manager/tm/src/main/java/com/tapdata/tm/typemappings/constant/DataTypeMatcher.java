package com.tapdata.tm.typemappings.constant;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/11 下午2:46
 */
@FunctionalInterface
public interface DataTypeMatcher {

    boolean match(Object expression, String dataType);

}
