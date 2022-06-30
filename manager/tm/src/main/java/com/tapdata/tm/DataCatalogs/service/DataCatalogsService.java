package com.tapdata.tm.DataCatalogs.service;

import com.google.common.collect.Maps;
import com.tapdata.tm.DataCatalogs.dto.DataCatalogsDto;
import com.tapdata.tm.DataCatalogs.entity.DataCatalogsEntity;
import com.tapdata.tm.DataCatalogs.repository.DataCatalogsRepository;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2022/01/24
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class DataCatalogsService extends BaseService<DataCatalogsDto, DataCatalogsEntity, ObjectId, DataCatalogsRepository> {
  private MetadataInstancesService metadataInstancesService;
  private WorkerService workerService;
  private MessageQueueService messageQueueService;

  public DataCatalogsService(@NonNull DataCatalogsRepository repository) {
    super(repository, DataCatalogsDto.class, DataCatalogsEntity.class);
  }

  protected void beforeSave(DataCatalogsDto dataCatalogs, UserDetail user) {

  }

  public List<Map> getList(Filter filter, UserDetail user) {
    if (filter != null && filter.getWhere() != null && filter.getWhere().get("listtags.id") != null) {
      Criteria criteria = Criteria.where("meta_type").in("collection")
        .and("is_deleted").is(false)
        .and("classifications.id").in(filter.getWhere().get("listtags.id"));
      List<MetadataInstancesDto> metadataInstancesDtos = metadataInstancesService.findAllDto(
        Query.query(criteria), user);

      if (CollectionUtils.isNotEmpty(metadataInstancesDtos)) {
        List<Map<String, Object>> conn_infos = new ArrayList<>();

        for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtos) {
          Map<String, Object> map = new HashMap();
          map.put("uri", metadataInstancesDto.getSource().getDatabase_uri());
          map.put("collection", metadataInstancesDto.getOriginalName());
          conn_infos.add(map);
        }
        Map<String, Object> connInfoMap = Maps.newHashMap();
        connInfoMap.put("$in", conn_infos);
        filter.getWhere().remove("listtags.id");
        if (filter.getWhere() != null && filter.getWhere().get("conn_info") != null) {
          filter.getWhere().put("conn_info", connInfoMap);
        }
      }
      return getData(filter,user);
    }else{
      return getData(filter,user);
      }
    }
    public List<Map> getData(Filter filter , UserDetail user){
      LookupOperation lookupOperation =LookupOperation.newLookup().from("Connections")
        .localField("connection_id")
        .foreignField("_id").as("source");
      Where where = filter.getWhere();
      Criteria criteria = repository.whereToCriteria(where);
      AggregationOperation match = Aggregation.match(criteria);
      LimitOperation limitOperation = Aggregation.limit(filter.getLimit());
      SkipOperation skipOperation = Aggregation.skip((long)filter.getSkip());
      Sort sort = null;
      for (String field : filter.getSort()) {
        if (StringUtils.isNotBlank(field)){
          String[] str = field.split("\\s+");
          if(str.length == 2)
            if (Sort.Direction.fromOptionalString(str[1]).isPresent()) {
              Sort and = Sort.by(str[1], str[0]);
              if (sort == null) {
                sort = and;
              } else {
                sort.and(and);
              }
            }
        }
      }
     /* SortOperation sortOperation = null;
      if (sort != null) {
        sortOperation = Aggregation.sort(sort);
      }*/
      ProjectionOperation project = Aggregation.project("_id", "conn_info", "user_id", "database"
        , "collection", "connection_id",
        "asset_desc", "total_docs", "violated_percentage"
        , "create_time", "lastModified", "tags", "violated_docs"
        /*"source.id"*/, "sourceName", "source.status", "source.database_type");
      Aggregation aggregation = Aggregation.newAggregation(lookupOperation, match, skipOperation, limitOperation,project);
      List<Map> metadataInstances = repository.getMongoOperations().aggregate(aggregation
        , "MetadataInstances", Map.class).getMappedResults();

      return metadataInstances;
    }
    public long getCount(Filter filter , UserDetail user){
    if (filter !=null&&filter.getWhere() !=null&&filter.getWhere().get("listtags.id")!=null){
      Criteria criteria = Criteria.where("meta_type").in("collection")
        .and("is_deleted").is(false)
        .and("classifications.id").in(filter.getWhere().get("listtags.id"));
      List<MetadataInstancesDto>metadataInstancesDtos =metadataInstancesService.findAllDto(Query.query(criteria),user);
      if (CollectionUtils.isNotEmpty(metadataInstancesDtos)){
      List<Map<String,Object>>conn_infos =new ArrayList<>();
      for (MetadataInstancesDto metadataInstancesDto:metadataInstancesDtos){
        Map<String,Object>map =new HashMap<>();
        map.put("uri",metadataInstancesDto.getSource().getDatabase_uri());
        map.put("collection",metadataInstancesDto.getOriginalName());
        conn_infos.add(map);
      }
      Map <String,Object>m =new HashMap<>();
      m.put("$in",conn_infos);
        filter.getWhere().remove("listtags.id");
        if (filter != null && filter.getWhere() != null && filter.getWhere().get("conn_info") != null) {
          filter.getWhere().put("conn_info", m);
        }
      }
      return getCountData(filter,user);
    }else {
      return getCountData(filter,user);
    }
    }
    public Integer getCountData(Filter filter, UserDetail user){
    LookupOperation lookupOperation =LookupOperation.newLookup().from("Connections")
      .localField("connection_id").foreignField("_id").as("source");
      Where where = filter.getWhere();
      Criteria criteria = repository.whereToCriteria(where);
      AggregationOperation match = Aggregation.match(criteria);
      CountOperation count = Aggregation.count().as("count");
      Aggregation aggregation = Aggregation.newAggregation(lookupOperation, match,count);
      List<Map> dataCategorys = repository.getMongoOperations().aggregate(aggregation
        , "MetadataInstances",Map.class).getMappedResults();
     Map map = dataCategorys.get(0);
     return  (Integer) map.get("count");

    }
    public List<String> distinctTag(UserDetail user){
      Query query = new Query();
      query.fields().include("tags");
      List<DataCatalogsEntity> tags = repository.findDistinct(new Query(), "tags", user, DataCatalogsEntity.class);
      /*List<String> list = new ArrayList<>();
      for (DataCatalogsEntity tag : tags) {
        list.add(tag.getTags());
      }*/
      List<String> collect = tags.stream().map(DataCatalogsEntity::getTags).collect(Collectors.toList());
      return collect;
    }

  public Boolean analyzeByConnId(List<String> ids, UserDetail user) {
    // 前端接口未使用，如果再使用的话 需要改成调用findAvailableAgentByAccessNode
    List<Worker> availableAgent = workerService.findAvailableAgent(user);
    // todo jacques 这里应该要采用 调度策略 后续补充上
    String processID = availableAgent.get(0).getProcessId();
    Map<String, Object> data = new HashMap<>();
    data.put("connectionIds", ids);
    data.put("type", "analyzeQuality");
    MessageQueueDto queueDto = new MessageQueueDto();
    queueDto.setReceiver(processID);
    queueDto.setData(data);
    queueDto.setType("pipe");
    messageQueueService.sendMessage(queueDto);
    return true;
  }
}
