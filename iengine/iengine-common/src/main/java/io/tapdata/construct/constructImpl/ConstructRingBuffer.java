package io.tapdata.construct.constructImpl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.persistence.PersistenceStorage;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.construct.ConstructIterator;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import org.bson.Document;

import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-02-07 11:11
 **/
public class ConstructRingBuffer<T extends Document> extends BaseConstruct<T> {

	public final static String SEQUENCE_KEY = "sequence";

	private Ringbuffer<Document> ringbuffer;

	public ConstructRingBuffer(HazelcastInstance hazelcastInstance, String name) {
		super(name);
		this.ringbuffer = hazelcastInstance.getRingbuffer(name);
	}

	public ConstructRingBuffer(HazelcastInstance hazelcastInstance, String name, ExternalStorageDto externalStorageDto) {
		super(name, externalStorageDto);
		ExternalStorageUtil.initHZRingBufferStorage(externalStorageDto, name, hazelcastInstance.getConfig());
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
		this.ringbuffer.destroy();
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
