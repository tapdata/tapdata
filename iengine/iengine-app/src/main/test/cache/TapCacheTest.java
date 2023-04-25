package cache;

import io.tapdata.flow.engine.V2.util.TapCache;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author aplomb
 */
public class TapCacheTest {
	@Test
	public void test() {
		AtomicBoolean switcher = new AtomicBoolean(true);
		TapCache<Boolean> cache = new TapCache<Boolean>().expireTime(1000L).disableCacheValue(false).supplier(switcher::get);

		for (int i = 0; i < 100; i++) {
			cache.get();
		}
		assert cache.getCacheHit() == 99;
		assert cache.getCacheMiss() == 1;
		assert cache.getCacheDisabled() == 0;

		cache = new TapCache<Boolean>().expireTime(1000L).disableCacheValue(false).supplier(switcher::get);
		for (int i = 0; i < 100; i++) {
			cache.get();
			if (i == 80) {
				switcher.set(false);
				try {
					Thread.sleep(1010L);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
		assert cache.getCacheHit() == 80;
		assert cache.getCacheMiss() == 20;
		assert cache.getCacheDisabled() == 19;
	}
}
