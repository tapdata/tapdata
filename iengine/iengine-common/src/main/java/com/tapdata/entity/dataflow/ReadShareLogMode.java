package com.tapdata.entity.dataflow;

/**
 * @author samuel
 * @Description 共享日志读取时候的模式
 * @create 2021-04-30 18:07
 **/
public enum ReadShareLogMode {

	/**
	 * 通过监听日志的形式，默认值，数量多了，对数据库会产生压力
	 */
	STREAMING,

	/**
	 * 轮询的形式，实时性没有STREAMING高，降低对数据库的压力
	 */
	POLLING
}
