package com.tapdata.processor;

import com.tapdata.entity.Connections;

import java.util.List;
import java.util.Map;

public interface ScriptConnection {

	void initialize(Connections connections);

	Connections getConnections();

	/**
	 * executeObj:
	 * {
	 * //  只对支持sql语句的数据库有效
	 * sql: "update order set owner='jk' where order_id=1",
	 * <p>
	 * // 以下对属性非sql数据库有效
	 * op: 'update'      // insert/ update/ delete/ findAndModify
	 * database:"inventory",
	 * collection:'orders',
	 * filter: {name: 'jackin'}  //  条件过滤对象
	 * opObject:  {$set:{data_quality: '100'}},    //   操作的数据集
	 * upsert: true,     // 是否使用upsert操作， 默认false，只对mongodb的update/ findAndModify有效
	 * multi: true        //  是否更新多条记录，默认false
	 * }
	 *
	 * @param executeObj
	 * @return
	 */
	long execute(Map<String, Object> executeObj);

	List<Map<String, Object>> executeQuery(Map<String, Object> executeObj);

	Object call(String funcName, List<Map<String, Object>> params);

	default String createIndex(Map<String, Object> index) {
		throw new UnsupportedOperationException("Unsupported create index operation.");
	}

	default void drop() {
		throw new UnsupportedOperationException("Unsupported drop collection operation.");
	}

	void close();

	boolean isClosed();
}
