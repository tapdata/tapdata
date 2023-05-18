package io.tapdata.observable.logging.with;

import io.tapdata.observable.logging.appender.Appender;

/**
 * @author GavinXiao
 * @description WithAppender create by Gavin
 * @create 2023/5/11 19:14
 **/
public interface WithAppender<T> {
    public Appender<T> append();
}
