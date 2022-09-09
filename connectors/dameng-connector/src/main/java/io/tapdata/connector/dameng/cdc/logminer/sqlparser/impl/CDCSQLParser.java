package io.tapdata.connector.dameng.cdc.logminer.sqlparser.impl;


import io.tapdata.connector.dameng.cdc.logminer.sqlparser.ISQLParser;
import io.tapdata.connector.dameng.cdc.logminer.sqlparser.contant.Operate;
import io.tapdata.connector.dameng.cdc.logminer.sqlparser.domain.ResultDO;
import io.tapdata.connector.dameng.cdc.logminer.sqlparser.util.IChecker;
import io.tapdata.connector.dameng.cdc.logminer.sqlparser.util.SQLReader;

import java.util.ArrayList;
import java.util.List;

/**
 * SQL解析器- CDC
 * <pre>
 * Author: <a href="mailto:harsen_lin@163.com">Harsen</a>
 * CreateTime: 2021/8/23 下午5:07
 * </pre>
 */
public class CDCSQLParser implements ISQLParser<String, ResultDO> {

  public char nameQuote = '"';
  public char valueQuote = '\'';
  public char escape = '\\';
  public IChecker isName = c -> { switch (c) {
  	case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7': case '8': case '9': case '_':
  	case 'a': case 'b': case 'c': case 'd': case 'e': case 'f': case 'g': case 'h': case 'i': case 'j': case 'k': case 'l': case 'm': case 'n': case 'o': case 'p': case 'q': case 'r': case 's': case 't': case 'u': case 'v': case 'w': case 'x': case 'y': case 'z':
	case 'A': case 'B': case 'C': case 'D': case 'E': case 'F': case 'G': case 'H': case 'I': case 'J': case 'K': case 'L': case 'M': case 'N': case 'O': case 'P': case 'Q': case 'R': case 'S': case 'T': case 'U': case 'V': case 'W': case 'X': case 'Y': case 'Z':
		return true;
	default: return false;
  } };
  public IChecker isNumber = c -> { switch (c) { case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7': case '8': case '9': case '.': return true; default: return false; } };
  public IChecker isSkip = c -> { switch (c) { case ' ': case '\t': case '\n': case '\r': return true; default: return false; } };

	public CDCSQLParser() {
	}

	@Override
  public ResultDO from(String sql) {
    SQLReader sr = SQLReader.build(sql);
    switch (sr.current()) {
      case 'i':
      case 'I':
        if (sr.equalsIgnoreCaseAndMove("insert") && sr.nextAndSkip(isSkip) && sr.equalsIgnoreCaseAndMove("into") && sr.nextAndSkip(isSkip)) {
          return insertBuild(sr);
        }
        break;
      case 'd':
      case 'D':
        if (sr.equalsIgnoreCaseAndMove("delete") && sr.nextAndSkip(isSkip)) {
          return deleteBuild(sr);
        }
        break;
      case 'u':
      case 'U':
        if (sr.equalsIgnoreCaseAndMove("update") && sr.nextAndSkip(isSkip)) {
          return updateBuild(sr);
        }
        break;
      default:
        break;
    }
    throw sr.ex("SQL must start with 'INSERT' or 'DELETE' or 'UPDATE'");
  }

  @Override
  public String to(ResultDO data) {
    return null;
  }

  protected ResultDO insertBuild(SQLReader sr) {
    ResultDO result = new ResultDO(Operate.Insert);
    setTableName(sr, result);

    List<String> names = loadInQuotaNames(sr);
    if ((sr.equalsIgnoreCaseAndMove("values") || sr.equalsIgnoreCaseAndMove("value")) && (sr.current('(') || sr.nextAndSkip(isSkip))) {
      sr.currentCheck('(', "Can't found '(' after values");
      sr.nextAndSkip(isSkip);

      result.putData(names.get(0), loadValue(sr));
      sr.nextAndSkip(isSkip);
      for (int i = 1, len = names.size(); i < len; i ++) {
        sr.currentCheck(',', "Can't found more values");
        sr.nextAndSkip(isSkip);
				result.putData(names.get(i), loadValue(sr));
				sr.nextAndSkip(isSkip);
      }
      sr.currentCheck(')', "Can't found ')' end values");
    } else {
      throw sr.ex("Can't found 'values' or value");
    }

    return result;
  }

  protected ResultDO deleteBuild(SQLReader sr) {
    ResultDO result = new ResultDO(Operate.Delete);
    if (!sr.equalsIgnoreCaseAndMove("from") || !sr.nextAndSkip(isSkip)) {
      throw sr.ex("Not found 'from' before table name");
    }
    setTableName(sr, result);
    setWhere(sr, result);
    return result;
  }

  protected ResultDO updateBuild(SQLReader sr) {
    String tmp;
    ResultDO result = new ResultDO(Operate.Update);

    setTableName(sr, result);
    if (!sr.equalsIgnoreCaseAndMove("set") || !sr.nextAndSkip(isSkip)) {
      throw sr.ex("Not found 'set' after table name");
    }

    // load set
    while (true) {
      tmp = loadName(sr, "Can't found column name");
      sr.nextAndSkip(isSkip);
      result.putData(tmp, loadConditionValue(sr));
      sr.nextAndSkip(isSkip);
      if (sr.current(',')) {
        sr.nextAndSkip(isSkip);
        continue;
      }
      break;
    }

    setWhere(sr, result);
    return result;
  }

  protected String loadName(SQLReader sr, String errorMsg) {
    if (sr.current(nameQuote)) {
      return sr.loadInQuote(50, escape);
    } else {
      return sr.loadIn(isName, errorMsg);
//      return sr.loadNotIn(isSplit);
    }
  }

  protected Object loadValue(SQLReader sr) {
    if (sr.current(valueQuote)) {
      return sr.loadInQuote(50, escape);
    } else if (isNumber.check(sr.current())) {
			return sr.loadIn(isNumber, "Can't found number value");
		} else {
			String tmp = loadName(sr, "Can't found function name");
			if ("true".equalsIgnoreCase(tmp)) {
				return true;
			} else if ("false".equalsIgnoreCase(tmp)) {
				return false;
			} else if ("null".equalsIgnoreCase(tmp)) {
				return null;
			} else if (sr.nextAndSkip(isSkip) && sr.current('(')) {
				return tmp + sr.loadInQuoteMulti(50, ')');
			}
			throw sr.ex("Value error '" + tmp + "'");
    }
  }

  protected Object loadConditionValue(SQLReader sr) {
    switch (sr.current()) {
      case '=':
        sr.nextAndSkip(isSkip);
        return loadValue(sr);
      case 'i':
      case 'I':
        if ('s' == sr.next() || 'S' == sr.current()) {
          sr.nextAndSkip(isSkip);
          return loadValue(sr);
        }
        break;
      default:
        break;
    }
    throw sr.ex("Not found condition");
  }

  protected List<String> loadInQuotaNames(SQLReader sr) {
    String tmp;
    List<String> names = new ArrayList<>();
    sr.currentCheck('(', "Can't found '(' after table name");
    do {
      sr.nextAndSkip(isSkip);
      tmp = loadName(sr, "Column name is null");
      sr.nextAndSkip(isSkip);
      names.add(tmp);
    } while (sr.current(','));
    sr.currentCheck(')', "Can't found ')' end columns");
    sr.nextAndSkip(isSkip);
    return names;
  }

  protected void setTableName(SQLReader sr, ResultDO result) {
    String tmp = loadName(sr, "Table name is null");
    sr.nextAndSkip(isSkip);
    if (sr.current('.')) {
      result.setSchema(tmp);
      sr.nextAndSkip(isSkip);
      tmp = loadName(sr, "Table name is null");
      sr.nextAndSkip(isSkip);
    }
    result.setTableName(tmp);
  }

  protected void setWhere(SQLReader sr, ResultDO result) {
    // load where
    if (!sr.equalsIgnoreCaseAndMove("where") || !sr.nextAndSkip(isSkip)) {
      throw sr.ex("Not found 'where'");
    }
    String tmp = loadName(sr, "Can't found column name");
    sr.nextAndSkip(isSkip);
    result.putData(tmp, loadConditionValue(sr));

    // set condition
    while (sr.nextAndSkip(isSkip)) {
      if (sr.current(';')) {
        break;
	    } else if (sr.equalsIgnoreCaseAndMove("and")) {
        sr.nextAndSkip(isSkip);
        tmp = loadName(sr, "Can't found column name");
        sr.nextAndSkip(isSkip);
        result.putIfAbsent(tmp, loadConditionValue(sr));
      } else {
        throw sr.ex("Condition not start 'and'");
      }
    }
  }
}
