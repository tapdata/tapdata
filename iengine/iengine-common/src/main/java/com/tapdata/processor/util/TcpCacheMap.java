package com.tapdata.processor.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.voovan.network.filter.StringFilter;
import org.voovan.network.tcp.TcpSocket;
import org.voovan.tools.collection.CacheMap;

import java.io.IOException;

public class TcpCacheMap extends CacheMap<String, TcpSocket> {

	private static Logger logger = LogManager.getLogger(TcpCacheMap.class);

	//初始化
	{
		super.supplier(k -> {
					String[] args = k.split(":");
					try {
						TcpSocket tcpSocket = new TcpSocket(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
						//启动服务类
						tcpSocket.filterChain().add(new StringFilter());
						tcpSocket.syncStart();
						return tcpSocket;
					} catch (IOException e) {
						logger.error("create tcpSocket error", e);
					}
					return null;
				}, true)
				.destory((key, tcpSocket) -> {
					logger.info("{} tcpSocket close", key);
					tcpSocket.close();
					return -1L;
				})
				//连接失效被清理
				.autoRemove(true)
				//最大连接100个
				.maxSize(100)
				//每60s检查一次
				.interval(60)
				//连接空闲5min后被回收
				.expire(300)
				.create();

	}

	public String getKey(String host, int port, int readTimeout) {
		return host + ":" + port + ":" + readTimeout;
	}

	public TcpSocket get(String host, int port, int readTimeout) {
		return super.getAndRefresh(getKey(host, port, readTimeout));
	}
}
