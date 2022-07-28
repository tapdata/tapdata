package io.tapdata.connector.entity;

import lombok.Getter;
import lombok.Setter;

/**
 * 缓存键配置
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/4 下午9:29 Create
 */
@Getter
@Setter
public class RedisKey {
  private String val;
  private String prefix;

  public RedisKey() {
  }

  public RedisKey(String val, String prefix) {
    this.val = val;
    this.prefix = prefix;
  }
}
