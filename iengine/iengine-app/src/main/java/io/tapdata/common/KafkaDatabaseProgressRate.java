package io.tapdata.common;

import com.mongodb.MongoClient;
import com.tapdata.constant.TapdataOffset;
import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.entity.ProgressRateStats;
import io.tapdata.Target;

import java.util.List;
import java.util.Map;

public class KafkaDatabaseProgressRate implements DatabaseProgressRate {
	@Override
	public ProgressRateStats progressRateInfo(Job job, Connections sourceConn, Connections targetConn, MongoClient targetMongoClient, List<Target> targets) {
		return null;
	}

	@Override
	public long getCdcLastTime(Connections sourceConn, Job job) {
		Object offset = job.getOffset();
		if (offset instanceof Map) {
			Map<String, Long> localOffsetMap = (Map<String, Long>) offset;
			Long timestamp = localOffsetMap.get("timestamp");
			if (timestamp != null) {
				return timestamp;
			}
		} else if (offset instanceof TapdataOffset) {
			TapdataOffset tapdataOffset = (TapdataOffset) offset;
			Object tapdataOffsetOffset = tapdataOffset.getOffset();
			if (tapdataOffsetOffset instanceof Map) {
				Map<String, Long> localOffsetMap = (Map<String, Long>) tapdataOffsetOffset;
				Long timestamp = localOffsetMap.get("timestamp");
				if (timestamp != null) {
					return timestamp;
				}
			}
		}
		return 0;
	}
}
