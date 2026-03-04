package com.tapdata.tm.commons.task.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CheckTaskMemoryResult {
    private String estimatedWithOverhead;
    private String safeThreshold;
    private String realFree;
    private String tableName;
    private String avgSize;
    private Long inFlightEffective;
    private Boolean isSafe;
    private Long batchSize;
    
    public static CheckTaskMemoryResult create(Double estimatedWithOverhead,Double safeThreshold,long realFree,String tableName,long avgSize,long inFlightEffective,long batchSize) {
        return CheckTaskMemoryResult.builder()
                .estimatedWithOverhead(human(estimatedWithOverhead.longValue()))
                .safeThreshold(human(safeThreshold.longValue()))
                .realFree(human(realFree))
                .tableName(tableName)
                .avgSize(human(avgSize))
                .inFlightEffective(inFlightEffective)
                .isSafe(estimatedWithOverhead < safeThreshold)
                .batchSize(batchSize)
                .build();
    }

    public static CheckTaskMemoryResult safe() {
        CheckTaskMemoryResult checkTaskMemoryResult = new CheckTaskMemoryResult();
        checkTaskMemoryResult.setIsSafe(true);
        return checkTaskMemoryResult;
    }


    private static String human(long bytes) {
        double v = bytes;
        String[] u = {"B","KB","MB","GB","TB"};
        int i = 0;
        while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
        return String.format(java.util.Locale.ROOT, "%.2f%s", v, u[i]);
    }
}
