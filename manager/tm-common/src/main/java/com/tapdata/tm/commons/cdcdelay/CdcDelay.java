package com.tapdata.tm.commons.cdcdelay;

import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Consumer;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/4 10:23 Create
 */
public class CdcDelay implements ICdcDelay {
    private static final Logger logger = LoggerFactory.getLogger(CdcDelay.class);

    private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");
    private static final Long INTERVAL = 5000L;

    private boolean isFilter;
    private long lastUpdated;

    public CdcDelay() {
        lastUpdated = System.currentTimeMillis();
    }

    @Override
    public void addHeartbeatTable(@NonNull List<String> tables) {
        if (tables.contains(ConnHeartbeatUtils.TABLE_NAME)) {
            isFilter = false;
            return;
        }
        tables.add(ConnHeartbeatUtils.TABLE_NAME);
        isFilter = true;
    }

    @Override
    public TapEvent filterAndCalcDelay(TapEvent tapEvent, @NonNull Consumer<Long> delayConsumer) {
        if (tapEvent instanceof TapRecordEvent) {
            Long sourceTimes;
            TapRecordEvent tapRecordEvent = ((TapRecordEvent) tapEvent);
            if (ConnHeartbeatUtils.TABLE_NAME.equals(tapRecordEvent.getTableId())) {
                sourceTimes = parseTs(tapRecordEvent);
                if (isFilter) {
                    HeartbeatEvent heartbeatEvent = new HeartbeatEvent();
                    tapEvent.clone(heartbeatEvent);
                    heartbeatEvent.setReferenceTime(sourceTimes);
                    tapEvent = heartbeatEvent;
                }
            } else {
                sourceTimes = tapRecordEvent.getReferenceTime();
            }

            // calc cdc delay, Does not limit the case where the result is negative, used to reflect the delay exception
            if (null != sourceTimes && (System.currentTimeMillis() - lastUpdated > INTERVAL)) {
                delayConsumer.accept(System.currentTimeMillis() - sourceTimes);
                lastUpdated = System.currentTimeMillis();
            }
        }

        return tapEvent;
    }

    /**
     * heartbeat record event times use 'after.ts' or 'referenceTime'
     *
     * @param recordEvent heartbeat record event
     * @return heartbeat event times
     */
    private static Long parseTs(@NonNull TapRecordEvent recordEvent) {
        Object ts = null;
        if (recordEvent instanceof TapInsertRecordEvent) {
            Map<String, Object> after = ((TapInsertRecordEvent) recordEvent).getAfter();
            if (null != after) {
                ts = after.get("ts");
            }
        } else if (recordEvent instanceof TapUpdateRecordEvent) {
            Map<String, Object> after = ((TapUpdateRecordEvent) recordEvent).getAfter();
            if (null != after) {
                ts = after.get("ts");
            }
        } else {
            return recordEvent.getReferenceTime();
        }
        if (ts instanceof Date) {
            return ((Date) ts).getTime();
        } else if (null != ts) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                sdf.setTimeZone(UTC_TIME_ZONE);
                Date date = sdf.parse(ts.toString());
                return date.getTime();
            } catch (ParseException e) {
                logger.warn("calc '{}' delay failed: {}", ts, e.getMessage());
            }
        }
        return recordEvent.getReferenceTime();
    }
}
