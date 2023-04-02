package com.tapdata.tm.commons.cdcdelay;

import io.tapdata.entity.event.TapEvent;
import org.springframework.lang.NonNull;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/4 10:18 Create
 */
public interface ICdcDelay {
    /**
     * add heartbeat table to cdc tables
     *
     * @param tables cdc tables
     */
    boolean addHeartbeatTable(@NonNull List<String> tables);

    /**
     * filter heartbeat event and calc delay times
     *
     * @param tapEvent TapEvent
     * @return TapEvent
     */
    TapEvent filterAndCalcDelay(TapEvent tapEvent, @NonNull Consumer<Long> delayConsumer);
}
