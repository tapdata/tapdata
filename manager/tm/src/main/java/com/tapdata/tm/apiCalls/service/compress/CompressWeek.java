package com.tapdata.tm.apiCalls.service.compress;

import com.tapdata.tm.apiCalls.entity.WorkerCallEntity;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.TemporalAdjusters;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/3 11:25 Create
 * @description
 */
@Service
public class CompressWeek implements Compress, InitializingBean {


    @Override
    public void afterPropertiesSet() {
        Compress.Factory.register(Type.WEEK, this);
    }

    @Override
    public Long compressTime(WorkerCallEntity e) {
        LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(e.getTimeStart()),
                ZoneId.systemDefault()
        );
        LocalDateTime monday = dateTime.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
        monday = monday.withHour(0).withMinute(0).withSecond(0).withNano(0);
        return monday.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    @Override
    public long plus(long time) {
        LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(time),
                ZoneId.systemDefault()
        );
        LocalDateTime monday = dateTime.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
        monday = monday.withHour(0).withMinute(0).withSecond(0).withNano(0);
        return monday.plusWeeks(1).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
}
