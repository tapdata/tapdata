package com.tapdata.constant;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * mongo client 代理类，用于在创建mongo连接时，加入自定义逻辑
 *
 * @author jackin
 * @date 2021/5/23 11:39 PM
 **/
public class MongoClientProxy extends MongoClient {

	private static Logger logger = LogManager.getLogger(MongoClientProxy.class);

	/**
	 * 记录已创建的mongodb的连接数
	 * key：线程方法调用栈的toString后生成的hash code
	 * value： 连接数统计数
	 */
	private static ConcurrentHashMap<String, AtomicInteger> aliveMongoClientCount = new ConcurrentHashMap<>();

	/**
	 * 记录每个mongo client对应 线程方法调用栈的toString后 的hash code
	 * key：this
	 * value：线程方法调用栈的toString后生成的hash code
	 */
	private static ConcurrentHashMap<MongoClient, String> aliveMongoClientHashCode = new ConcurrentHashMap<>();

	public MongoClientProxy(MongoClientURI uri) {
		super(uri);
		final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
		StringBuilder sb = new StringBuilder();
		if (stackTrace != null && stackTrace.length > 0) {
			for (StackTraceElement stackTraceElement : stackTrace) {
				sb.append(stackTraceElement.toString());
			}

			final String hashCode = String.valueOf(sb.toString().hashCode());

			synchronized (hashCode.intern()) {
				if (!aliveMongoClientCount.containsKey(hashCode)) {
					aliveMongoClientCount.put(hashCode, new AtomicInteger(0));
				}

				aliveMongoClientCount.get(hashCode).incrementAndGet();

				aliveMongoClientHashCode.put(this, hashCode);

				final int aliveCount = aliveMongoClientCount.get(hashCode).get();
				if (aliveCount % 10 == 0) {
					logger.warn("Alive mongo client threshold warning, alive count {}, call stack {}.", aliveCount, sb.toString());
				}
			}

		}
	}

	@Override
	public void close() {
		try {
			super.close();
		} finally {
			if (aliveMongoClientHashCode.containsKey(this)) {
				final String hashCode = aliveMongoClientHashCode.get(this);

				if (aliveMongoClientCount.containsKey(hashCode)) {
					final int aliveCount = aliveMongoClientCount.get(hashCode).decrementAndGet();
					if (aliveCount <= 0) {
						synchronized (hashCode.intern()) {
							if (aliveCount <= 0) {
								aliveMongoClientCount.remove(hashCode);
							}
						}
					}
				}

				aliveMongoClientHashCode.remove(this);
			}
		}
	}
}
