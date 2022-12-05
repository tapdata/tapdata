package io.tapdata.websocket.handler;

import com.tapdata.tm.commons.ping.PingDto;
import io.tapdata.websocket.EventHandlerAnnotation;

import java.util.LinkedList;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-11-10 16:15
 **/
@EventHandlerAnnotation(type = "pong")
public class PongHandler extends BaseEventHandler {
	private final static int MAX_SIZE = 100;
	private static final LinkedList<Map<String, Object>> cacheList;
	public static final int MAX_WAIT_MS = 15000;

	static {
		cacheList = new LinkedList<>();
	}

	@Override
	public Object handle(Map event) {
		if (null == event) return null;
		synchronized (cacheList) {
			if (cacheList.size() >= MAX_SIZE) {
				cacheList.remove(0);
			}
			cacheList.add(event);
		}
		return null;
	}

	public static boolean handleResponse(String pingId, Consumer<Map<String, Object>> consumer) {
		long currentTimeMillis = System.currentTimeMillis();
		Map<String, Object> cache;
		while (true) {
			synchronized (cacheList) {
				cache = cacheList.stream().filter(map -> map.get("pingId").toString().equals(pingId)).findFirst().orElse(null);
			}
			if (null != cache) {
				if (!cache.containsKey(PingDto.PING_RESULT)) {
					logger.error("Missing field '{}'", PingDto.PING_RESULT);
				} else {
					consumer.accept(cache);
				}
				removeCache(cache);
				break;
			}
			if (System.currentTimeMillis() - currentTimeMillis > MAX_WAIT_MS) {
				break;
			}
			try {
				Thread.sleep(100L);
			} catch (InterruptedException ignored) {
				break;
			}
		}
		return null != cache;
	}

	public static void removeCache(Map<String, Object> map) {
		if (null == map) return;
		synchronized (cacheList) {
			cacheList.remove(map);
		}
	}
}
