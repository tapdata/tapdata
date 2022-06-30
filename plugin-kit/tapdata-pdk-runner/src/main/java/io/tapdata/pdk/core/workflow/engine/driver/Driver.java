package io.tapdata.pdk.core.workflow.engine.driver;

import io.tapdata.entity.codec.filter.Replacer;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.core.utils.queue.SingleThreadBlockingQueue;
import io.tapdata.pdk.core.workflow.engine.driver.task.TaskManager;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class Driver {
    protected TaskManager taskManager;
    private List<SingleThreadBlockingQueue<List<TapEvent>>> queues = new CopyOnWriteArrayList<>();

    public void registerQueue(SingleThreadBlockingQueue<List<TapEvent>> queue) {
        if(!queues.contains(queue))
            queues.add(queue);
    }

    public void offer(List<TapEvent> events) {
        offer(events, null);
    }

    public void offer(List<TapEvent> events, Replacer<List<TapEvent>> replacer) {
        boolean disableClone = queues.size() <= 1; //performance optimization. If only one queue, don't clone events.
        for(SingleThreadBlockingQueue<List<TapEvent>> queue : queues) {
            if(replacer != null)
                queue.offer(replacer.replace(events, !disableClone));
            else
                queue.offer(events);
        }
    }

    public void destroy() {}
}
