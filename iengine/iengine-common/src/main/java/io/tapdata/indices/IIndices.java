package io.tapdata.indices;

import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.TableIndex;
import com.tapdata.entity.TableIndexTypeEnums;

import java.util.List;
import java.util.Map;

/**
 * 索引操作实例接口
 * <pre>
 * Author: <a href="mailto:linhs@thoughtup.cn">Harsen</a>
 * CreateTime: 2021/4/14 下午1:13
 * </pre>
 *
 * @param <T> 连接类型
 */
public interface IIndices<T> {

	/**
	 * 加载索引
	 *
	 * @param conn   连接
	 * @param schema 模式
	 * @param table  表信息
	 * @throws Exception 异常
	 */
	void load(T conn, String schema, RelateDataBaseTable table) throws Exception;

	/**
	 * 批量加载索引
	 *
	 * @param conn   连接
	 * @param schema 模式
	 * @param tables 表信息
	 * @throws Exception 异常
	 */
	default void load(T conn, String schema, List<RelateDataBaseTable> tables) throws Exception {
		throw new UnsupportedOperationException("Operation does not supported");
	}

	/**
	 * 加载全部索引
	 *
	 * @param conn     连接
	 * @param schema   模式
	 * @param indexMap 所有索引
	 * @throws Exception 异常
	 */
	void loadAll(T conn, String schema, Map<String, Map<String, TableIndex>> indexMap) throws Exception;

	/**
	 * 创建索引
	 *
	 * @param conn       连接
	 * @param schema     模式
	 * @param tableName  表名
	 * @param tableIndex 索引信息
	 * @throws Exception 异常
	 */
	void create(T conn, String schema, String tableName, TableIndex tableIndex) throws Exception;

	/**
	 * 判断索引是否存在
	 *
	 * @param conn      连接
	 * @param schema    模式
	 * @param tableName 表名
	 * @param indexName 索引名
	 * @return 存在
	 * @throws Exception 异常
	 */
	boolean exist(T conn, String schema, String tableName, String indexName) throws Exception;

	/**
	 * 转标准索引
	 *
	 * @param sourceIndexTypeStr 源索引
	 * @return 标准索引
	 */
	TableIndexTypeEnums toIndexType(String sourceIndexTypeStr);

	/**
	 * 转源索引
	 *
	 * @param indexType 标准索引
	 * @return 源索引(null ： 表示使用默认索引 ， 长度0 ： 表示未实现的索引 ， 有值 ： 数据库具体索引)
	 */
	String toSourceIndexType(TableIndexTypeEnums indexType);

	/**
	 * 转源索引
	 *
	 * @param indexTypeStr 标准索引
	 * @return 源索引
	 */
	default String toSourceIndexType(String indexTypeStr) {
		// indexTypeStr could be null, so using `TableIndexTypeEnums.valueOf(indexTypeStr)` will cause NPE
		TableIndexTypeEnums indexType = TableIndexTypeEnums.parse(indexTypeStr);
		return toSourceIndexType(indexType);
	}

	/**
	 * 去双引号
	 *
	 * @param str 字符串
	 * @return 去双引号后的字符串
	 */
	default String unquote(String str) {
		if (null != str && !str.isEmpty()) {
			if (str.startsWith("\"") && str.endsWith("\"")) {
				StringBuilder buf = new StringBuilder(str.length() - 2);
				for (char c : str.substring(1, str.length() - 1).toCharArray()) {
					if ('\\' == c) continue;
					buf.append(c);
				}
				return buf.toString();
			}
		}

		return str;
	}
}
