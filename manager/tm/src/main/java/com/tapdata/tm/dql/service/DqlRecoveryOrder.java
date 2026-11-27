package com.tapdata.tm.dql.service;

import com.tapdata.tm.dql.dto.DqlEventDto;

import java.util.Comparator;
import java.util.Date;
import java.util.List;

/** Defines the single server-side order used by preview, batches and recovery dispatch. */
final class DqlRecoveryOrder {
    private static final Comparator<DqlEventDto> COMPARATOR = Comparator
            .comparing(DqlEventDto::getTaskId, Comparator.nullsFirst(String::compareTo))
            .thenComparing(DqlEventDto::getEventTime, Comparator.nullsLast(Date::compareTo))
            .thenComparing(DqlEventDto::getCaptureSeq, Comparator.nullsLast(Long::compareTo))
            .thenComparing(DqlEventDto::getEventId, Comparator.nullsLast(String::compareTo));

    private DqlRecoveryOrder() {
    }

    static List<DqlEventDto> sort(List<DqlEventDto> events) {
        if (events == null || events.isEmpty()) {
            return List.of();
        }
        return events.stream().sorted(COMPARATOR).toList();
    }
}
