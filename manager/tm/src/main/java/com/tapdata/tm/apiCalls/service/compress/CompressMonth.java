package com.tapdata.tm.apiCalls.service.compress;

import com.tapdata.tm.apiCalls.entity.WorkerCallEntity;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/3 11:26 Create
 * @description
 */
@Service
public class CompressMonth implements Compress, InitializingBean {

    @Override
    public void afterPropertiesSet() {
        Compress.Factory.register(Type.MONTH, this);
    }

    @Override
    public Long compressTime(WorkerCallEntity e) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(e.getTimeStart()), ZoneId.systemDefault());
        return localDateTime.toLocalDate().withDayOfMonth(1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }

    @Override
    public long plus(long time) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault());
        return localDateTime.toLocalDate().withDayOfMonth(1).plusMonths(1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }
}
