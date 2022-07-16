package io.tapdata.constructImpl;

import io.tapdata.HazelcastConstruct;

/**
 * @author samuel
 * @Description
 * @create 2022-02-09 14:39
 **/
public class BaseConstruct<T> implements HazelcastConstruct<T> {

  protected Integer ttlSecond;

  protected BaseConstruct() {
  }

  protected void convertTtlDay2Second(Integer ttlDay) {
    if (ttlDay != null && ttlDay > 0) {
      this.ttlSecond = ttlDay * 24 * 60 * 60;
    }
  }
}
