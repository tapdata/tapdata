package io.tapdata.connector.dameng.cdc.logminer.sqlparser;

/**
 * SQL解析器
 * <pre>
 * Author: <a href="mailto:harsen_lin@163.com">Harsen</a>
 * CreateTime: 2021/8/21 下午6:46
 * </pre>
 */
public interface ISQLParser<L, D> {
  D from(L sql);

  String to(D data);
}
