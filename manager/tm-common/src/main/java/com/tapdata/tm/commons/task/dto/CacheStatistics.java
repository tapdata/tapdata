package com.tapdata.tm.commons.task.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CacheStatistics {
    private Number size;
    private Number count;
    private Number countLimit;
    private String uri;
    private String method;

    public static CacheStatistics createLocalCache(Number size, Number count, Number countLimit) {
        return CacheStatistics.builder()
                .size(size)
                .count(count)
                .countLimit(countLimit)
                .method("Local")
                .build();
    }

    public static CacheStatistics createRemoteCache(Number size, Number count,String uri,String mode) {
        return CacheStatistics.builder()
                .size(size)
                .count(count)
                .uri(uri)
                .method(mode)
                .build();
    }



}
