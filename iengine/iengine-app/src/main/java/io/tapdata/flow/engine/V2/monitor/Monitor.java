package io.tapdata.flow.engine.V2.monitor;

import java.io.Closeable;

/**
 * @author samuel
 * @Description
 * @create 2022-03-02 01:30
 **/
public interface Monitor<T> extends Closeable {
  default void start() {
    throw new UnsupportedOperationException();
  }

  default T get(){
    throw new UnsupportedOperationException();
  }
}
