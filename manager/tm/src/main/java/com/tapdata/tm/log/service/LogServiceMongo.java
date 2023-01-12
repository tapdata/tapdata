package com.tapdata.tm.log.service;

import cn.hutool.core.bean.BeanUtil;
import com.tapdata.manager.common.utils.IOUtils;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.log.dto.LogDto;
import com.tapdata.tm.log.entity.LogEntity;
import com.tapdata.tm.log.repository.LogRepository;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.util.CloseableIterator;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author:
 * @Date: 2021/09/14
 * @Description:
 */
@Service
@Slf4j
public class LogServiceMongo extends BaseService<LogDto, LogEntity, ObjectId, LogRepository> {
    public LogServiceMongo(@NonNull LogRepository repository) {
        super(repository, LogDto.class, LogEntity.class);
    }

    protected void beforeSave(LogDto logs, UserDetail user) {

    }

    public void save(LogDto logDto){
        LogEntity log=new LogEntity();
        BeanUtil.copyProperties(logDto,log);
        repository.getMongoOperations().save(log);
    }

    public void export(Number start, Number end, String dataFlowId, OutputStream outputStream) {
        if (start == null) {
            start = new Date().getTime() - 1000 * 60 * 60 * 24; // 默认取一天
        }

        Criteria criteria = Criteria.where("millis").gte(start);
        if (end != null) {
            criteria.and("millis").lte(end);
        }
        criteria.and("contextMap.dataFlowId").is(dataFlowId);

        Query query = Query.query(criteria);
        query.with(Sort.by("millis").ascending());
        query.cursorBatchSize(1000);
        query.allowSecondaryReads();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        AtomicInteger count = new AtomicInteger(0);
        CloseableIterator<LogEntity> it = repository.getMongoOperations().stream(query, LogEntity.class);
        it.forEachRemaining(log -> {
            StringBuffer sb = new StringBuffer();

            sb.append("[").append(log.getLevel()).append("]");
            if (log.getDate() instanceof Long || log.getMillis() instanceof Long) {
                sb.append(" ").append(sdf.format(new Date(
                        (Long)(log.getMillis() != null ? log.getMillis() : log.getDate())
                )));
            } else {
                sb.append(" [invalid log datetime]");
            }
            sb.append(" ").append(log.getThreadName());
            sb.append(" ").append(log.getLoggerName());
            sb.append(" ").append(log.getMessage());

            sb.append("\n");
            count.addAndGet(1);

            try {
                outputStream.write(sb.toString().getBytes());
            } catch (IOException e) {
                throw new BizException("Export.IOError", e);
            }
        });
        IOUtils.closeQuietly(it);

        if (count.get() == 0) {
            try {
                outputStream.write(("Can't find any logs by query " + query.getQueryObject().toJson()).getBytes());
            } catch (IOException e) {
                throw new BizException("Export.IOError", e);
            }
        }

    }
}
