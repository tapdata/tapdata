package io.tapdata.flow.engine.V2.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * @author aplomb
 */
public class TapCache<T> {
	private T cached;
	private volatile long cacheTime;
	private Set<T> disableCacheValues;
	private long expireTime;
	private LongAdder cacheHit = new LongAdder();
	private LongAdder cacheMiss = new LongAdder();
	private LongAdder cacheDisabled = new LongAdder();
	public TapCache<T> expireTime(long expireTime) {
		this.expireTime = expireTime;
		return this;
	}
	private Supplier<T> supplier;
	public TapCache<T> supplier(Supplier<T> supplier) {
		this.supplier = supplier;
		return this;
	}
	public TapCache<T> disableCacheValue(T... ts) {
		if(ts != null && ts.length > 0) {
			if(disableCacheValues == null)
				disableCacheValues = new HashSet<>();
			disableCacheValues.addAll(Arrays.asList(ts));
		}
		return this;
	}

	public T get() {
		T value = cached;
		long oldCacheTime = cacheTime;
		if(value != null) {
			long time = System.currentTimeMillis();
			if(time - cacheTime > expireTime) {
				value = null;
			} else {
				cacheHit.increment();
				return value;
			}
		}
		if(supplier != null) {
			if(cached == null || oldCacheTime == cacheTime) {
				synchronized (this) {
					if(oldCacheTime == cacheTime) {
						value = supplier.get();
						cacheMiss.increment();
						cacheTime = System.currentTimeMillis();
						if(disableCacheValues == null || !disableCacheValues.contains(value)) {
							cached = value;
						} else {
							cacheDisabled.increment();
							cached = null;
						}
					}
				}
			}
		}
		if(value == null) {
			cacheHit.increment();
			value = cached;
		}
		return value;
	}

	public Set<T> getDisableCacheValues() {
		return disableCacheValues;
	}

	public long getExpireTime() {
		return expireTime;
	}

	public long getCacheHit() {
		return cacheHit.longValue();
	}

	public long getCacheMiss() {
		return cacheMiss.longValue();
	}

	public long getCacheDisabled() {
		return cacheDisabled.longValue();
	}
}
