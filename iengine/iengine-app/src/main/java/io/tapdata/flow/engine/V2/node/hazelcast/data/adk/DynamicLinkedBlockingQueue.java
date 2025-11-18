package io.tapdata.flow.engine.V2.node.hazelcast.data.adk;

import com.google.common.collect.Queues;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/11/17 10:56 Create
 * @description The capacity of a queue is not an iron wall,
 * But rather the rhythm of the ebb and flow of the tide.
 * Change it without the need for rough locks,
 * Just gently push away that moment in the wind.
 * The new queue catches the future,
 * The old queue walked past,
 * Migration whispers softly in the dark,
 * Performance runs wildly in the light.
 */
public class DynamicLinkedBlockingQueue<E> {
    private static final int MAX_CAPACITY = 100000;
    private static final int MIN_CAPACITY = 100;

    private static class QueueHolder<E> {
        final LinkedBlockingQueue<E> queue;
        final LinkedBlockingQueue<E> migratingQueue;
        final boolean migrating;

        QueueHolder(LinkedBlockingQueue<E> queue) {
            this.queue = queue;
            this.migratingQueue = null;
            this.migrating = false;
        }

        QueueHolder(LinkedBlockingQueue<E> oldQueue, LinkedBlockingQueue<E> newQueue) {
            this.queue = newQueue;
            this.migratingQueue = oldQueue;
            this.migrating = true;
        }
    }

    public interface IsAlive {
        boolean test();
    }

    private final AtomicReference<QueueHolder<E>> holderRef;
    private final ExecutorService migrator = Executors.newSingleThreadExecutor();
    private volatile int capacity;
    private IsAlive active;

    private int fixCapacity(int capacity) {
        if (capacity <= 0) {
            capacity = MIN_CAPACITY;
        }
        if (capacity >= MAX_CAPACITY) {
            capacity = MAX_CAPACITY;
        }
        return capacity;
    }

    /**
     * create a DynamicLinkedBlockingQueue with capacity
     * @param capacity the capacity of the queue, if capacity <= 0, the capacity will be set to MIN_CAPACITY,
     * if capacity >= MAX_CAPACITY, the capacity will be set to MAX_CAPACITY
     * */
    public DynamicLinkedBlockingQueue(int capacity) {
        capacity = fixCapacity(capacity);
        holderRef = new AtomicReference<>(new QueueHolder<>(new LinkedBlockingQueue<>(capacity)));
        this.capacity = capacity;
    }

    public DynamicLinkedBlockingQueue<E> active(IsAlive active) {
        this.active = active;
        return this;
    }

    private boolean active() {
        return active != null && active.test();
    }

    public int capacity() {
        return this.capacity;
    }

    /**
     * change the capacity of the queue, if the new capacity is less than the current capacity, do nothing
     * @param newSize the new capacity of the queue, if newSize <= 0, the newSize will be set to MIN_CAPACITY,
     *                if newSize >= MAX_CAPACITY, the newSize will be set to MAX_CAPACITY.
     *                if newSize is less than the current capacity, do nothing
     * */
    public int changeTo(int newSize, int sourceQueueFactor) {
        newSize = fixCapacity(newSize);
        if (this.capacity >= newSize) {
            return this.capacity;
        }
        QueueHolder<E> oldHolder = holderRef.get();
        LinkedBlockingQueue<E> newQueue = new LinkedBlockingQueue<>((newSize * sourceQueueFactor) >> 1);
        this.capacity = newSize;
        QueueHolder<E> newHolder = new QueueHolder<>(oldHolder.queue, newQueue);
        if (!holderRef.compareAndSet(oldHolder, newHolder)) {
            return newSize;
        }
        migrator.submit(() -> migrate(oldHolder.queue, newQueue));
        return newSize;
    }

    private void migrate(LinkedBlockingQueue<E> oldQ, LinkedBlockingQueue<E> newQ) {
        E e;
        while ((e = oldQ.poll()) != null && this.active()) {
            newQ.offer(e);
        }
    }

    public boolean offer(E e) {
        return holderRef.get().queue.offer(e);
    }

    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        return holderRef.get().queue.offer(e, timeout, unit);
    }

    public E poll() throws InterruptedException {
        while (this.active()) {
            QueueHolder<E> h = holderRef.get();
            E v = h.queue.poll();
            if (v != null) return v;
            if (h.migrating) {
                v = h.migratingQueue.poll();
                if (v != null) return v;
            }
            return h.queue.take();
        }
        return null;
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        while (this.active()) {
            QueueHolder<E> h = holderRef.get();
            E v = h.queue.poll();
            if (v != null) return v;
            if (h.migrating) {
                v = h.migratingQueue.poll(timeout, unit);
                if (v != null) return v;
            }
            return h.queue.take();
        }
        return null;
    }

    public int size() {
        QueueHolder<E> h = holderRef.get();
        return h.queue.size() + (h.migrating ? h.migratingQueue.size() : 0);
    }

    public boolean isEmpty() {
        QueueHolder<E> h = holderRef.get();
        return h.queue.isEmpty() && (!h.migrating || h.migratingQueue.isEmpty());
    }

    public int drain(Collection<E> accept, int maxElements, long timeout, TimeUnit unit) throws InterruptedException {
        if (isEmpty()) {
            return 0;
        }
        QueueHolder<E> h = holderRef.get();
        int drain = 0;
        if (h.migrating) {
            drain = Queues.drain(h.migratingQueue, accept, maxElements, 1L, unit);
        }
        if (drain < maxElements) {
            drain += Queues.drain(h.queue, accept, maxElements - drain, timeout, unit);
        }
        return drain;
    }

    public LinkedBlockingQueue<E> getQueue() {
        return holderRef.get().queue;
    }
}
