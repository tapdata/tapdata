package io.tapdata.flow.engine.V2.sharecdc.impl;

import com.tapdata.constant.ClientOperatorUtil;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.SettingService;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcContext;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcReader;
import io.tapdata.flow.engine.V2.sharecdc.exception.ShareCdcUnsupportedException;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * @author samuel
 * @Description
 * @create 2022-02-17 15:35
 **/
public class ShareCdcBaseReader implements ShareCdcReader {

	protected static final int DEFAULT_QUEUE_SIZE = 100;
	protected static final Logger logger = LogManager.getLogger(ShareCdcBaseReader.class);

	protected ShareCdcContext shareCdcContext;
	protected ClientMongoOperator clientMongoOperator;
	protected AtomicBoolean running;
	protected LinkedBlockingQueue<ShareCDCReaderEvent> queue;
	protected Throwable throwable;

	protected ShareCdcBaseReader() {
	}

	@Override
	public void init(ShareCdcContext shareCdcContext) throws ShareCdcUnsupportedException {
		this.shareCdcContext = shareCdcContext;
		this.clientMongoOperator = ClientOperatorUtil.buildHttpClientMongoOperator(shareCdcContext.getConfigurationCenter());
		this.queue = new LinkedBlockingQueue<>(DEFAULT_QUEUE_SIZE);
		this.running = new AtomicBoolean(true);

		// Check global share cdc setting
		if (!ShareCdcUtil.shareCdcEnable(new SettingService(clientMongoOperator))) {
			throw new ShareCdcUnsupportedException("Global share cdc disabled", true);
		}
	}

	@Override
	public boolean isRunning() {
		return running.get();
	}

	@Override
	public void close() throws IOException {
		this.running.compareAndSet(true, false);
	}

	protected void handleFailed(String err) {
		if (StringUtils.isNotBlank(err)) {
			this.throwable = new Throwable(err);
		}
	}

	protected void handleFailed(Throwable throwable) {
		if (throwable != null) {
			this.throwable = throwable;
		}
	}

	protected void handleFailed(String err, Throwable throwable) {
		if (throwable != null) {
			this.throwable = StringUtils.isNotBlank(err) ? new Throwable(err, throwable) : throwable;
		}
	}

	protected void enqueue(ShareCDCReaderEvent shareCDCReaderEvent) {
		if (shareCDCReaderEvent == null) {
			return;
		}
		while (running.get()) {
			try {
				if (this.queue.offer(shareCDCReaderEvent, 3L, TimeUnit.SECONDS)) {
					break;
				}
			} catch (InterruptedException e) {
				break;
			}
		}
	}

	protected void poll(StreamReadConsumer streamReadConsumer) throws Exception {
		AtomicBoolean firstEvent = new AtomicBoolean(false);
		while (running.get()) {
			ShareCDCReaderEvent shareCDCReaderEvent;
			if (this.throwable != null) {
				throw new Exception(throwable.getMessage(), throwable);
			}
			try {
				shareCDCReaderEvent = queue.poll(3, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				break;
			}
			if (shareCDCReaderEvent == null) {
				continue;
			}
			if (firstEvent.compareAndSet(false, true)) {
				TapEvent tapEvent = shareCDCReaderEvent.getTapEvent();
				logger.info("Received first log\n - op: " + TapEventUtil.getOp(tapEvent)
						+ "\n - table: " + TapEventUtil.getTableId(tapEvent)
						+ "\n - timestamp: " + TapEventUtil.getTimestamp(tapEvent)
						+ "\n - time string: " + Instant.ofEpochMilli(TapEventUtil.getTimestamp(tapEvent))
						+ "\n - offset: " + shareCDCReaderEvent.getOffsetObj()
						+ "\n - before: " + TapEventUtil.getBefore(tapEvent)
						+ "\n - after: " + TapEventUtil.getAfter(tapEvent));
			}
			List<TapEvent> events = new ArrayList<>();
			events.add(shareCDCReaderEvent.getTapEvent());
			streamReadConsumer.accept(events, shareCDCReaderEvent.getOffsetObj());
		}
	}

	protected static void logDocumentVerify(Document document) {
		if (null == document) {
			throw new IllegalArgumentException("Document is null");
		}
		List<String> missingField = new ArrayList<>();
		if (!document.containsKey("op")) {
			missingField.add("op");
		}
		if (!document.containsKey("offsetString")) {
			missingField.add("offsetString");
		}
		if (CollectionUtils.isNotEmpty(missingField)) {
			throw new IllegalArgumentException("Log data unusable, missing field: " + String.join(",", missingField) + ", document: " + document);
		}
	}

	protected static class ShareCDCReaderEvent implements Serializable {
		private static final long serialVersionUID = -1412447971238523627L;
		private final TapEvent tapEvent;
		private final Object offsetObj;

		public ShareCDCReaderEvent(TapEvent tapEvent, Object offsetObj) {
			this.tapEvent = tapEvent;
			this.offsetObj = offsetObj;
		}

		public TapEvent getTapEvent() {
			return tapEvent;
		}

		public Object getOffsetObj() {
			return offsetObj;
		}
	}
}
