package io.tapdata.construct.constructImpl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.PersistenceStorage;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.construct.ConstructIterator;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * @author samuel
 * @Description
 * @create 2022-02-07 11:11
 **/
public class ConstructRingBuffer<T extends Document> extends BaseConstruct<T> {
	private final static Logger logger = LogManager.getLogger(ConstructRingBuffer.class);
	public final static String SEQUENCE_KEY = "sequence";
	private final Ringbuffer<Document> ringbuffer;

	public ConstructRingBuffer(HazelcastInstance hazelcastInstance, String name) {
		super(name);
		this.ringbuffer = hazelcastInstance.getRingbuffer(name);
		long headSequence = this.ringbuffer.headSequence();
		long tailSequence = this.ringbuffer.tailSequence();
		if (logger.isDebugEnabled()) {
			logger.debug("Create construct ringbuffer succeed. Name: {}, head: {}, tail: {}", this.ringbuffer.getName(), headSequence, tailSequence);
		}
	}

	public ConstructRingBuffer(HazelcastInstance hazelcastInstance, String referenceId, String name, ExternalStorageDto externalStorageDto) {
		super(referenceId, name, externalStorageDto);
		ExternalStorageUtil.initHZRingBufferStorage(externalStorageDto, referenceId, name, hazelcastInstance.getConfig());
		this.ringbuffer = hazelcastInstance.getRingbuffer(name);
		Integer ttlDay = externalStorageDto.getTtlDay();
		if (ttlDay != null && ttlDay > 0) {
			convertTtlDay2Second(ttlDay);
			PersistenceStorage.getInstance().setRingBufferTTL(this.ringbuffer, this.ttlSecond);
		}
	}

	@Override
	public int insert(T data) throws Exception {
		this.ringbuffer.add(data);
		return 1;
	}

	@Override
	public void destroy() throws Exception {
		if (PersistenceStorage.getInstance().destroy(referenceId, ConstructType.RINGBUFFER, name) && null != ringbuffer) {
			ringbuffer.destroy();
		}
	}

	@Override
	public long findSequence(long timestamp) throws Exception {
		return PersistenceStorage.getInstance().findSequence(this.ringbuffer, timestamp);
	}

	public Ringbuffer<Document> getRingbuffer() {
		return ringbuffer;
	}

	@Override
	public ConstructIterator<T> find() throws Exception {
		return new RingBufferIterator(ringbuffer, 0);
	}

	@Override
	public ConstructIterator<T> find(Map<String, Object> filter) throws Exception {
		long sequence = this.ringbuffer.headSequence();
		if (filter != null && filter.containsKey(SEQUENCE_KEY)) {
			try {
				sequence = Long.parseLong(filter.get(SEQUENCE_KEY).toString());
			} catch (NumberFormatException e) {
				throw new Exception("Filter is invalid, should be {\"" + SEQUENCE_KEY + "\", some-long-value}");
			}
		}
		return new RingBufferIterator(ringbuffer, sequence);
	}

	@Override
	public boolean isEmpty() {
		if (this.ringbuffer == null) {
			return true;
		}
		return this.ringbuffer.headSequence() == 0 && this.ringbuffer.tailSequence() == -1;
	}

	@Override
	public long insertMany(List<T> data, Predicate<Void> stop) throws Exception {
		if (null == this.ringbuffer) {
			return 0;
		}
		AtomicLong count = new AtomicLong();
		AtomicReference<RuntimeException> error = new AtomicReference<>();
		CountDownLatch countDownLatch = new CountDownLatch(1);
		this.ringbuffer.addAllAsync(data, OverflowPolicy.OVERWRITE)
				.whenComplete((c, err) -> {
					try {
						if (null != err) {
							error.set(new RuntimeException(err));
						}
						count.set(c);
					} finally {
						countDownLatch.countDown();
					}
				});
		CommonUtils.countDownAwait(stop, countDownLatch);
		if (null != error.get()) {
			throw error.get();
		}
		return count.get();
	}

	@Override
	public String getName() {
		return this.ringbuffer.getName();
	}

	@Override
	public String getType() {
		return "RingBuffer";
	}

	static class RingBufferIterator<E extends Document> implements ConstructIterator<E> {

		private Ringbuffer<E> ringbuffer;
		private long sequence;

		public RingBufferIterator(Ringbuffer<E> ringbuffer, long sequence) {
			assert ringbuffer != null;
			assert sequence >= 0;
			this.ringbuffer = ringbuffer;
			this.sequence = sequence;
		}

		@Override
		public E tryNext() {
			if (hasNext()) {
				return next();
			} else {
				return null;
			}
		}

		@Override
		public List<E> tryNextMany(int maxCount, Predicate<Void> stop) {
			if (!hasNext()) {
				return null;
			}
			CountDownLatch countDownLatch = new CountDownLatch(1);
			AtomicReference<RuntimeException> err = new AtomicReference<>();
			List<E> list = new ArrayList<>();
			ringbuffer.readManyAsync(sequence, 1, maxCount, null)
					.whenComplete((rs, throwable) -> {
						try {
							if (null != throwable) {
								err.set(new RuntimeException(String.format("Read ring buffer many async failed, start sequence: %s, min count: %s, max count: %s", sequence, 1, maxCount), throwable));
							}
							rs.forEach(list::add);
						} finally {
							countDownLatch.countDown();
						}
					});
			CommonUtils.countDownAwait(stop, countDownLatch);
			if (null != err.get()) {
				throw err.get();
			}
			return list;
		}

		@Override
		public boolean hasNext() {
			long tailSequence = ringbuffer.tailSequence();
			return sequence <= tailSequence;
		}

		@Override
		public E peek() {
			E e = null;
			try {
				e = ringbuffer.readOne(sequence < 0 ? 0 : sequence);
			} catch (InterruptedException ignore) {
			}
			return e;
		}

		@Override
		public E peek(long timeout, TimeUnit timeUnit) {
			long endTs = System.currentTimeMillis() + timeUnit.toMillis(timeout);
			E e;
			while (true) {
				e = peek();
				if (null != e) {
					break;
				}
				if (System.currentTimeMillis() > endTs) {
					break;
				}
				try {
					TimeUnit.MILLISECONDS.sleep(10L);
				} catch (InterruptedException ignored) {
					break;
				}
			}
			return e;
		}

		@Override
		public E next() {
			E e = peek();
			sequence++;
			return e;
		}

		@Override
		public long getSequence() {
			return sequence;
		}
	}
}
