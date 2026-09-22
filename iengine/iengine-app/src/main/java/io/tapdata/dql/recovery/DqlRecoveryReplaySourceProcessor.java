package io.tapdata.dql.recovery;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.tapdata.entity.TapdataEvent;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

/**
 * A source-only Jet processor backed by a temporary Hazelcast queue.  It has
 * no connector and therefore cannot read from a business data source.  The
 * queue remains open until the recovery runtime closes the temporary job.
 */
public final class DqlRecoveryReplaySourceProcessor extends AbstractProcessor {
    private final String queueName;
    private transient IQueue<TapdataEvent> queue;
    private TapdataEvent pending;

    public DqlRecoveryReplaySourceProcessor(String queueName) {
        if (queueName == null || queueName.isBlank()) {
            throw new IllegalArgumentException("recovery queue name must not be blank");
        }
        this.queueName = queueName;
    }

    @Override
    protected void init(@NotNull Processor.Context context) throws Exception {
        super.init(context);
        HazelcastInstance hazelcastInstance = context.hazelcastInstance();
        queue = hazelcastInstance.getQueue(queueName);
    }

    @Override
    public boolean complete() {
        if (pending != null) {
            if (!tryEmit(pending)) {
                return false;
            }
            pending = null;
        }
		TapdataEvent event;
		try {
			event = queue == null ? null : queue.poll(100L, TimeUnit.MILLISECONDS);
		} catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			return false;
		}
		if (event == null) {
			return false;
		}
        if (!tryEmit(event)) {
            pending = event;
        }
        return false;
    }
}
