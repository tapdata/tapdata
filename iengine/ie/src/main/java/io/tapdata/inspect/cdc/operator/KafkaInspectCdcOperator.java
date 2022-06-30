package io.tapdata.inspect.cdc.operator;

import com.tapdata.entity.Connections;
import com.tapdata.entity.inspect.InspectCdcWinData;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 增量源操作实例 - Kafka
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/11/3 上午11:53 Create
 */
public class KafkaInspectCdcOperator extends AbsInspectCdcOperator {
	private final static Logger logger = LogManager.getLogger(ShareLogInspectCdcOperator.class);
	private Connections connections;
	private String taskId;
	private String topicName;

	public KafkaInspectCdcOperator(boolean isSource, String taskId, Connections connections, String topicName) {
		super(isSource);
		this.taskId = taskId;
		this.topicName = topicName;
		this.connections = connections;
	}

	@Override
	public long count(InspectCdcWinData cdcWinData) {
		long totals = 0;
//    ConsumerConfiguration consumerConf = new ConsumerConfiguration(connections, taskId, true);
//    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConf.build())) {
//      // 指定消费分区
//      Collection<TopicPartition> topicPartitions = getPartitions(topicName);
//      consumer.assign(topicPartitions);
//      // 初始化偏移量
//      long firstOffset = parseOffset(cdcWinData.getBeginOffset(), 0);
//      cdcWinData.setBeginOffset(String.valueOf(firstOffset));
//      cdcWinData.setEndOffset(String.valueOf(firstOffset));
//      Map<String, Long> offsetMap = getOffsetMap(topicName, firstOffset);
//      RuntimeOffsetListener runtimeOffsetListener = new RuntimeOffsetListener(consumer, consumerConf, offsetMap);
//      runtimeOffsetListener.onPartitionsAssigned(topicPartitions);
//
//      ConsumerRecord<byte[], byte[]> record;
//      ConsumerRecords<byte[], byte[]> records;
//      Iterator<ConsumerRecord<byte[], byte[]>> it;
//      tagConsumerPoll:
//      while (null != (records = consumer.poll(consumerConf.getPollTimeout()))) {
//        if (records.isEmpty()) {
//          logger.warn("No more data, break count.");
//          break;
//        }
//
//        it = records.iterator();
//        while (it.hasNext()) {
//          record = it.next();
//          cdcWinData.setEndOffset(String.valueOf(record.offset()));
//          // 数据在窗口前，丢弃
//          if (record.timestamp() < cdcWinData.getWinBegin().toEpochMilli()) continue;
//          // 数据在窗口后，结束
//          if (record.timestamp() >= cdcWinData.getWinEnd().toEpochMilli()) break tagConsumerPoll;
//          // 数据开始结束时间设置
//          if (0 == totals) cdcWinData.setDataBegin(Instant.ofEpochMilli(record.timestamp()));
//          cdcWinData.setDataEnd(Instant.ofEpochMilli(record.timestamp()));
//          totals++;
//        }
//      }
//    }

		return totals;
	}

	@Override
	public Instant lastEventDate() {
		throw new RuntimeException("未实现");
	}

	@Override
	public void close() throws Exception {
	}

	private Collection<TopicPartition> getPartitions(String topicName) {
		TopicPartition topicPartition = new TopicPartition(topicName, 0);
		return Collections.singletonList(topicPartition);
	}

	private Map<String, Long> getOffsetMap(String topicName, long offset) {
		Map<String, Long> offsetMap = new HashMap<>();
		offsetMap.put(topicName, offset);
		return offsetMap;
	}

	private long parseOffset(String offset, long defOffset) {
		try {
			return Long.parseLong(offset.trim());
		} catch (Exception e) {
			logger.warn("Offset is invalid, set value to " + defOffset);
			return defOffset;
		}
	}

}
