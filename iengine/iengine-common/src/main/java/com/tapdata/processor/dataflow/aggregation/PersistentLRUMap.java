package com.tapdata.processor.dataflow.aggregation;

import org.apache.commons.collections.map.LRUMap;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.function.Consumer;

/**
 * Persistent LRU存储类
 * 当存储数据超过设置的阈值时，会按照LRU规则将最旧的数据持久化到外存中
 *
 * @author jackin
 * @date 2020/11/28 8:39 PM
 **/
@NotThreadSafe
public class PersistentLRUMap extends LRUMap {
	/**
	 * 执行LRU时，触发调用该函数，用于监听LRU事件
	 */
	private Consumer<Entry> onRemoveLRU;

	public PersistentLRUMap(
			int maxSize,
			Consumer<Entry> onRemoveLRU) {

		super(maxSize);

		this.onRemoveLRU = onRemoveLRU;
	}

	/**
	 * 清除过期数据时，会调用该防范
	 *
	 * @param entry
	 * @return true: 数据可以被清理；false：数据不允许被清理
	 */
	@Override
	protected boolean removeLRU(LinkEntry entry) {
		if (onRemoveLRU != null) {
			onRemoveLRU.accept(entry);
		}
		return super.removeLRU(entry);
	}

	@Override
	public void clear() {
		HashEntry[] data = this.data;
		for (int i = data.length - 1; i >= 0; i--) {
			onRemoveLRU.accept(data[i]);
		}
		super.clear();
	}

	@Override
	public Object remove(Object key) {
		Object o = super.remove(key);
		if (o != null) {
			onRemoveLRU.accept((Entry) o);
		}
		return o;
	}
}
