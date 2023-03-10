package com.tapdata.tm.shareCdcTableMetrics.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.TmPageable;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.shareCdcTableMetrics.ShareCdcTableMetricsDto;
import com.tapdata.tm.shareCdcTableMetrics.entity.ShareCdcTableMetricsEntity;
import com.tapdata.tm.shareCdcTableMetrics.repository.ShareCdcTableMetricsRepository;
import com.tapdata.tm.utils.Lists;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2023/03/09
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class ShareCdcTableMetricsService extends BaseService<ShareCdcTableMetricsDto, ShareCdcTableMetricsEntity, ObjectId, ShareCdcTableMetricsRepository> {

    private DataSourceService sourceService;

    public ShareCdcTableMetricsService(@NonNull ShareCdcTableMetricsRepository repository) {
        super(repository, ShareCdcTableMetricsDto.class, ShareCdcTableMetricsEntity.class);
    }

    protected void beforeSave(ShareCdcTableMetricsDto shareCdcTableMetrics, UserDetail user) {

    }

    public Page<ShareCdcTableMetricsDto> getPageInfo(String taskId, int page, int size) {
        TmPageable pageable = new TmPageable();
        pageable.setPage(page);
        pageable.setSize(size);

        Criteria criteria = Criteria.where("taskId").is(taskId);
        Query query = Query.query(criteria);

        long count = count(query);
        if (count == 0) {
            return new Page<>(count, Lists.newArrayList());
        }
        query.with(Sort.by(Sort.Direction.DESC, "currentEventTime"));
        query.with(pageable);

        // TODO: 2023/3/9 need add group by
        List<ShareCdcTableMetricsDto> list = findAll(query);
        List<String> connectionIds = list.stream().map(ShareCdcTableMetricsDto::getConnectionId).collect(Collectors.toList());

        List<DataSourceConnectionDto> connectionDtos = sourceService.findAllByIds(connectionIds);
        Map<ObjectId, String> nameMap = connectionDtos.stream().collect(Collectors.toMap(DataSourceConnectionDto::getId, DataSourceConnectionDto::getName, (existing, replacement) -> existing));

        list.forEach(info -> info.setConnectionId(nameMap.get(new ObjectId(info.getConnectionId()))));

        return new Page<>(count, list);
    }

    public void saveOrUpdateDaily(ShareCdcTableMetricsDto shareCdcTableMetricsDto, UserDetail userDetail) {
        if (StringUtils.isBlank(shareCdcTableMetricsDto.getTaskId())) {
            throw new IllegalArgumentException("Task id cannot be empty");
        }
        if (StringUtils.isBlank(shareCdcTableMetricsDto.getNodeId())) {
            throw new IllegalArgumentException("Node id cannot be empty");
        }
        if (StringUtils.isBlank(shareCdcTableMetricsDto.getConnectionId())) {
            throw new IllegalArgumentException("Connection id cannot be empty");
        }
        if (StringUtils.isBlank(shareCdcTableMetricsDto.getTableName())) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }
        Criteria criteria = new Criteria().andOperator(
                Criteria.where("taskId").is(shareCdcTableMetricsDto.getTaskId()),
                Criteria.where("nodeId").is(shareCdcTableMetricsDto.getNodeId()),
                Criteria.where("connectionId").is(shareCdcTableMetricsDto.getConnectionId()),
                Criteria.where("tableName").is(shareCdcTableMetricsDto.getTableName())
        );
        Query query = Query.query(criteria).with(Sort.by(Sort.Direction.DESC, "_id")).limit(1);
        ShareCdcTableMetricsEntity lastShareCdcTableMetrics = repository.findOne(query, userDetail).orElse(null);
        Operation operation = Operation.INSERT;
        if (null != lastShareCdcTableMetrics) {
            LocalDate today = LocalDate.now(ZoneId.systemDefault());
            Date createAt = lastShareCdcTableMetrics.getCreateAt();
            if (null != createAt) {
                LocalDate lastDate = LocalDateTime.ofInstant(createAt.toInstant(), ZoneId.systemDefault()).toLocalDate();
                if (today.equals(lastDate)) {
                    operation = Operation.UPDATE;
                }
            }
        }
        switch (operation) {
            case INSERT:
                shareCdcTableMetricsDto.setCreateAt(new Date());
                if (null == shareCdcTableMetricsDto.getCount()
                        || shareCdcTableMetricsDto.getCount().compareTo(0L) < 0) {
                    shareCdcTableMetricsDto.setCount(0L);
                }
                if (null != lastShareCdcTableMetrics) {
                    shareCdcTableMetricsDto.setAllCount(lastShareCdcTableMetrics.getAllCount() + shareCdcTableMetricsDto.getCount());
                    shareCdcTableMetricsDto.setStartCdcTime(lastShareCdcTableMetrics.getStartCdcTime());
                    if (null == shareCdcTableMetricsDto.getCurrentEventTime()) {
                        shareCdcTableMetricsDto.setCurrentEventTime(lastShareCdcTableMetrics.getCurrentEventTime());
                    }
                } else {
                    shareCdcTableMetricsDto.setAllCount(shareCdcTableMetricsDto.getCount());
                    if (null == shareCdcTableMetricsDto.getCurrentEventTime()) {
                        shareCdcTableMetricsDto.setCurrentEventTime(0L);
                    }
                }
                ShareCdcTableMetricsEntity shareCdcTableMetricsEntity = convertToEntity(ShareCdcTableMetricsEntity.class, shareCdcTableMetricsDto);
                repository.insert(shareCdcTableMetricsEntity, userDetail);
                if (log.isDebugEnabled()) {
                    log.debug("Insert share cdc table metrics: {}", shareCdcTableMetricsEntity);
                }
                break;
            case UPDATE:
                Long count = shareCdcTableMetricsDto.getCount();
                if (null == count || count.compareTo(0L) < 0) {
                    break;
                }
                shareCdcTableMetricsDto.setCount(lastShareCdcTableMetrics.getCount() + count);
                shareCdcTableMetricsDto.setAllCount(lastShareCdcTableMetrics.getAllCount() + count);
                query = Query.query(Criteria.where("_id").is(lastShareCdcTableMetrics.getId()));
                Update update = new Update().set("count", shareCdcTableMetricsDto.getCount())
                        .set("allCount", shareCdcTableMetricsDto.getAllCount())
                        .set("currentEventTime", shareCdcTableMetricsDto.getCurrentEventTime());
                repository.update(query, update, userDetail);
                if (log.isDebugEnabled()) {
                    log.debug("Update share cdc table metrics, query: {}, update: {}", query.getQueryObject().toJson(), update.getUpdateObject().toJson());
                }
                break;
            default:
                break;
        }
    }

    private enum Operation{
        INSERT,
        UPDATE,
    }
}