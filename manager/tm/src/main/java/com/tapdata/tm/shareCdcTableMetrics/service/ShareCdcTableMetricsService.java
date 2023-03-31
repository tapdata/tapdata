package com.tapdata.tm.shareCdcTableMetrics.service;

import cn.hutool.extra.cglib.CglibUtil;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.shareCdcTableMetrics.ShareCdcTableMetricsDto;
import com.tapdata.tm.shareCdcTableMetrics.entity.ShareCdcTableMetricsEntity;
import com.tapdata.tm.shareCdcTableMetrics.entity.ShareCdcTableMetricsVo;
import com.tapdata.tm.shareCdcTableMetrics.repository.ShareCdcTableMetricsRepository;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.Lists;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import javax.print.DocFlavor;
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
    private MongoTemplate mongoTemplate;
    private TaskService taskService;

    public ShareCdcTableMetricsService(@NonNull ShareCdcTableMetricsRepository repository) {
        super(repository, ShareCdcTableMetricsDto.class, ShareCdcTableMetricsEntity.class);
    }

    protected void beforeSave(ShareCdcTableMetricsDto shareCdcTableMetrics, UserDetail user) {

    }

    public Page<ShareCdcTableMetricsDto> getPageInfo(String taskId, String nodeId, String tableTaskId, String keyword, int page, int size) {
        size = Math.min(size, 100);

        Criteria criteria = Criteria.where("taskId").is(taskId);
        if (StringUtils.isNotBlank(nodeId)) {
            criteria.and("nodeId").is(nodeId);
        }
        if (StringUtils.isNotBlank(keyword)) {
            criteria.and("tableName").regex(keyword);
        }
        if (StringUtils.isNotBlank(tableTaskId)) {
            TaskDto dto = taskService.findById(new ObjectId(tableTaskId));
            List<String> tableNames = Lists.newArrayList();
            dto.getDag().getSourceNodes().forEach(node -> {
                if (node instanceof TableNode) {
                    tableNames.add((((TableNode) node).getTableName()));
                } else if (node instanceof DatabaseNode) {
                    tableNames.addAll(((DatabaseNode) node).getTableNames());
                }
            });
            criteria.and("tableName").in(tableNames);
        }

        MatchOperation match = Aggregation.match(criteria);
        SortOperation sort = Aggregation.sort(Sort.by("currentEventTime").descending());
        GroupOperation group = Aggregation.group("taskId", "connectionId", "tableName")
                .first("taskId").as("taskId")
                .first("nodeId").as("nodeId")
                .first("connectionId").as("connectionId")
                .first("tableName").as("tableName")
                .first("startCdcTime").as("startCdcTime")
                .first("currentEventTime").as("currentEventTime")
                .first("count").as("count")
                .first("allCount").as("allCount");
        List<String> tableNameList = mongoTemplate.findDistinct(Query.query(criteria), "tableName", ShareCdcTableMetricsEntity.class, String.class);

        if (CollectionUtils.isEmpty(tableNameList)) {
            return new Page<>(0, Lists.newArrayList());
        }
        SkipOperation skip = Aggregation.skip(page - 1);
        LimitOperation limit = Aggregation.limit(size);
        Aggregation aggregation = Aggregation.newAggregation(match, sort, group, sort, skip, limit);
        AggregationResults<ShareCdcTableMetricsVo> metrics = mongoTemplate.aggregate(aggregation, "ShareCdcTableMetrics", ShareCdcTableMetricsVo.class);
        List<ShareCdcTableMetricsVo> list = metrics.getMappedResults();

        List<String> connectionIds = list.stream().map(ShareCdcTableMetricsVo::getConnectionId).collect(Collectors.toList());

        List<DataSourceConnectionDto> connectionDtos = sourceService.findAllByIds(connectionIds);
        Map<ObjectId, String> nameMap = connectionDtos.stream().collect(Collectors.toMap(DataSourceConnectionDto::getId, DataSourceConnectionDto::getName, (existing, replacement) -> existing));

        List<ShareCdcTableMetricsDto> result = list.stream().map(info -> {
            ShareCdcTableMetricsDto copy = CglibUtil.copy(info, ShareCdcTableMetricsDto.class);
            copy.setConnectionName(nameMap.get(new ObjectId(info.getConnectionId())));
            return copy;
        }).collect(Collectors.toList());

        return new Page<>(tableNameList.size(), result);
    }

    public List<ShareCdcTableMetricsVo> getCollectInfoByTaskId(String taskId) {
        MatchOperation match = Aggregation.match(Criteria.where("taskId").is(taskId));
        SortOperation sort = Aggregation.sort(Sort.by("currentEventTime").descending());
        GroupOperation group = Aggregation.group("taskId", "connectionId", "tableName")
                .first("taskId").as("taskId")
                .first("nodeId").as("nodeId")
                .first("connectionId").as("connectionId")
                .first("tableName").as("tableName");
        AggregationResults<ShareCdcTableMetricsVo> tableMetrics = mongoTemplate.aggregate(Aggregation.newAggregation(match, sort, group), "ShareCdcTableMetrics", ShareCdcTableMetricsVo.class);
        return tableMetrics.getMappedResults();
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

    public void deleteByTaskId(String taskId) {
        deleteAll(Query.query(Criteria.where("taskId").is(taskId)));
    }
}