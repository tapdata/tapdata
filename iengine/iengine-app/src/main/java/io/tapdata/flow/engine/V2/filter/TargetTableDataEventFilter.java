package io.tapdata.flow.engine.V2.filter;

import com.tapdata.entity.TapdataEvent;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;


public class TargetTableDataEventFilter implements TapEventFilter<TargetTableDataEventFilter.TapEventHandel, TapdataEvent> {
    private static final Logger logger = LogManager.getLogger(TargetTableDataEventFilter.class);
    List<TapEventHandel> handelList = new CopyOnWriteArrayList<>();
    public static TargetTableDataEventFilter create() {
        TargetTableDataEventFilter tapEventFilter = new TargetTableDataEventFilter();
        return tapEventFilter;
    }

    @Override
    public void addFilter(TapEventHandel tapEventPredicate) {
        handelList.add(tapEventPredicate);
    }


    @Override
    public <E extends TapdataEvent> TapdataEvent test(E event) {
        if (CollectionUtils.isEmpty(handelList)) {
            return event;
        }
        for (TapEventHandel handel : handelList) {
            event = (E) handel.handler(event);
        }
        return event;
    }

    public interface TapEventHandel {
        TapdataEvent handler(TapdataEvent event);
    }
}
