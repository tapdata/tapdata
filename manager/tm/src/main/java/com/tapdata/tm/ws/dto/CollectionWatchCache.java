/**
 * @title: WatchCache
 * @description:
 * @author lk
 * @date 2021/9/16
 */
package com.tapdata.tm.ws.dto;

import com.tapdata.tm.utils.MapUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.data.mongodb.core.messaging.MessageListenerContainer;

public class CollectionWatchCache {

	private MessageListenerContainer container;  // changestream container

	private Map<String, Map<String, Map<String, String>>> receiverInfo;  // 匹配的字段名称、值和receiver

	private Long cacheEmptyTime;

	public MessageListenerContainer getContainer() {
		return container;
	}

	public Map<String, Map<String, Map<String, String>>> getReceiverInfo() {
		return receiverInfo;
	}

	public Long getCacheEmptyTime() {
		return cacheEmptyTime;
	}

	public void setContainer(MessageListenerContainer container) {
		this.container = container;
	}

	public void setReceiverInfo(Map<String, Map<String, Map<String, String>>> receiverInfo) {
		this.receiverInfo = receiverInfo;
	}

	public void setCacheEmptyTime(Long cacheEmptyTime) {
		this.cacheEmptyTime = cacheEmptyTime;
	}

	/**
	 * @param key 字段名称
	 * @param value 字段值
	 * @param receiver receiver
	 * @description
	 **/
	public void addReceiverInfo(String key, List<String> value, String receiver, String sessionId){
		if (MapUtils.isEmpty(receiverInfo)){
			receiverInfo = new ConcurrentHashMap<>();
		}
		// 缓存前先清理已存在的数据
		removeReceiverInfo(sessionId);
		Map<String, Map<String, String>> map = receiverInfo.computeIfAbsent(key, k -> new ConcurrentHashMap<>());

		value.forEach(v -> {
			if (!map.containsKey(v)) {
				map.put(v, new HashMap<>());
			}
			map.get(v).put(sessionId, receiver);
		});
		setCacheEmptyTime(0L);
	}

	public void removeReceiverInfo(String sessionId){
		if (MapUtils.isNotEmpty(receiverInfo)) {
			int size = 0;
			for (Map<String, Map<String, String>> value : receiverInfo.values()) {
				if (MapUtils.isNotEmpty(value)) {
					value.values().forEach(map -> map.remove(sessionId));
				}
				value.entrySet().removeIf(entry -> MapUtils.isEmpty(entry.getValue()));
				size += value.size();
			}
			if (size == 0){
				setCacheEmptyTime(System.currentTimeMillis());
			}
		}
	}

	public int getReceiverInfoSize(){
		int size = 0;
		if (MapUtils.isNotEmpty(receiverInfo)) {
			size = receiverInfo.values().stream().mapToInt(Map::size).sum();
		}
		return size;
	}

}
