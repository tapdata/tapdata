package io.tapdata.sybase.cdc.dto.analyse.stream;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.dto.analyse.filter.ReadFilter;

import java.util.*;
import java.util.concurrent.*;

class TextAccepter implements Accepter {
    ReadFilter readFilter;
    CdcRoot root;
    Log log;
    private StreamReadConsumer cdcConsumer;
    private ConcurrentHashMap<String, EventEntity> monitorFilePathQueues;
    private ScheduledFuture<?> futureCheckFile;
    private final ScheduledExecutorService scheduledExecutorServiceCheckFile = Executors.newSingleThreadScheduledExecutor();
    Map<String, Set<String>> blockFieldsMap;
    Map<String, TapTable> tapTableMap;

    final Object acceptLock = new Object();

    TextAccepter(Map<String, Set<String>> blockFieldsMap, Map<String, TapTable> tapTableMap) {
        this.blockFieldsMap = blockFieldsMap;
        this.tapTableMap = tapTableMap;
        monitorFilePathQueues = new ConcurrentHashMap<>();
        this.futureCheckFile = this.scheduledExecutorServiceCheckFile.scheduleWithFixedDelay(() -> {
            synchronized (acceptLock) {
                try {
                    ConcurrentHashMap.KeySetView<String, EventEntity> tables = monitorFilePathQueues.keySet();
                    if (!tables.isEmpty()) {
                        for (String fullTableName : tables) {
                            final Set<String> blockFields = Optional.ofNullable(blockFieldsMap.get(fullTableName)).orElse(new HashSet<>());
                            EventEntity eventEntity = monitorFilePathQueues.get(fullTableName);
                            accept(eventEntity.events, fullTableName, eventEntity.offset, blockFields);
                            eventEntity.events = new ArrayList<>();
                        }
                    }
                } catch (Throwable t) {
                    root.getThrowableCatch().set(t);
                }
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    @Override
    public void accept(String fullTableName, List<TapEvent> events, Object offset) {
        final Set<String> blockFields = Optional.ofNullable(blockFieldsMap.get(fullTableName)).orElse(new HashSet<>());
        if (blockFields.isEmpty()) {
            cdcConsumer.accept(events, offset);
            return;
        }
        if (events.size() == 1000) {
            accept(events, fullTableName, offset, blockFields);
            return;
        }
        synchronized (acceptLock) {
            EventEntity eventEntity = monitorFilePathQueues.containsKey(fullTableName) ?
                    monitorFilePathQueues.get(fullTableName)
                    : monitorFilePathQueues.computeIfAbsent(fullTableName, key -> new EventEntity());
            for (TapEvent event : events) {
                eventEntity.add(event, offset);
                if (eventEntity.events.size() == 1000) {
                    accept(eventEntity.events, fullTableName, offset, blockFields);
                    eventEntity.events = new ArrayList<>();
                }
            }
        }
    }

    private void accept(List<TapEvent> events, String fullTableName, Object offset, Set<String> blockFields) {
        final TapTable tableInfo = tapTableMap.get(fullTableName);
        cdcConsumer.accept(readFilter.readFilter(events, tableInfo, blockFields, fullTableName), offset);
    }

    @Override
    public void setStreamReader(StreamReadConsumer cdcConsumer) {
        this.cdcConsumer = cdcConsumer;
    }

    @Override
    public void close() {
        try {
            Optional.ofNullable(futureCheckFile).ifPresent(f -> f.cancel(true));
        } catch (Exception e) {
            log.debug("Read Blob fields's values process stop fail, msg: {}", e.getMessage());
        } finally {
            futureCheckFile = null;
        }
    }

    @Override
    public void setFilter(ReadFilter readFilter){
        this.readFilter = readFilter;
    }

    @Override
    public void setRoot(CdcRoot root){
        this.root = root;
        this.log = root.getContext().getLog();
    }

    class EventEntity {
        List<TapEvent> events;
        Object offset;
        EventEntity (List<TapEvent> e, Object o) {
            events = e;
            offset = o;
        }

        EventEntity () {
            events = new ArrayList<>();
        }

        public List<TapEvent> getEvents() {
            return events;
        }

        public void setEvents(List<TapEvent> events) {
            this.events = events;
        }

        public Object getOffset() {
            return offset;
        }

        public void setOffset(Object offset) {
            this.offset = offset;
        }

        public void add(List<TapEvent> events, Object offset) {
            if (null == events || null == offset) return;
            if (null == this.events ) this.events = new ArrayList<>();
            this.events.addAll(events);
            this.offset = offset;
        }

        public void add(TapEvent events, Object offset) {
            if (null == events || null == offset) return;
            if (null == this.events ) this.events = new ArrayList<>();
            this.events.add(events);
            this.offset = offset;
        }
    }
}
