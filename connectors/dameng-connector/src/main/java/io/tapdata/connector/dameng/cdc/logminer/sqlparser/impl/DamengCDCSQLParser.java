package io.tapdata.connector.dameng.cdc.logminer.sqlparser.impl;

import io.tapdata.connector.dameng.cdc.logminer.sqlparser.util.SQLReader;

/**
 * Oracle增量SQL解析
 * <pre>
 * Author: <a href="mailto:harsen_lin@163.com">Harsen</a>
 * CreateTime: 2021/8/24 下午4:15
 * </pre>
 */
public class DamengCDCSQLParser extends CDCSQLParser {

	public DamengCDCSQLParser() {
	}

  @Override
	protected Object loadValue(SQLReader sr) {
    if (sr.current(valueQuote)) {
      return sr.loadInQuote(50);
    } else {
      String tmp = loadName(sr, "Can't found function name");
      if ("Error".equals(tmp)) {// 解决Oracle源表日期类型有异常值导致的logminer返回Error Translating
        // Error Translating没有quote包裹, 所以position后移一位
        sr.moveTo(sr.position() + 1);
        if (sr.equalsAndMove(" Translating")) {
          // 前一步后移一位的, 要回退一位
          sr.moveTo(sr.position() - 1);
          return "Error Translating";
        } else {
          // 前一步后移一位的, 要回退一位
          sr.moveTo(sr.position() - 1);
        }
      } else {
        // 如果不是Error Translating问题, 需要position回退tmp的长度
        sr.moveTo(sr.position() - tmp.length() + 1);
      }
    }
    return super.loadValue(sr);
  }
}
