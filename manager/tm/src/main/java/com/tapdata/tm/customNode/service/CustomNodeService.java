package com.tapdata.tm.customNode.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.customNode.dto.CustomNodeDto;
import com.tapdata.tm.customNode.entity.CustomNodeEntity;
import com.tapdata.tm.customNode.repository.CustomNodeRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.Lists;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2022/03/09
 * @Description:
 */
@Service
@Slf4j
public class CustomNodeService extends BaseService<CustomNodeDto, CustomNodeEntity, ObjectId, CustomNodeRepository> {
 @Autowired
 private FileService fileService1;

 private final TaskService taskService;

  public CustomNodeService(@NonNull CustomNodeRepository repository,
                           TaskService taskService) {
    super(repository, CustomNodeDto.class, CustomNodeEntity.class);
    this.taskService = taskService;
  }

  protected void beforeSave(CustomNodeDto logs, UserDetail user) {
  }
  public String upload(MultipartFile file, UserDetail userDetail){

    try{
        ObjectId objectId =fileService1.storeFile(file.getInputStream(),file.getOriginalFilename(),null,new HashMap<>());
        if (objectId != null) {
          return objectId.toHexString();
        }
    } catch (IOException e) {
        e.printStackTrace();
      }

    return null;
  }
  public void uploadAndView(ObjectId id, UserDetail user, HttpServletResponse response) {
    String sourceId = id.toHexString();
    Criteria userCriteria = Criteria.where(sourceId).is(user.getCustomerId());
    Criteria scopeCriteria = Criteria.where("scope").is("public");


    Criteria criteria = Criteria.where(String.valueOf(userCriteria)).is(scopeCriteria);

    Query query = new Query(criteria);
    query.fields().include("_id");
    //long count = dataSourceDefinitionService.count(query);
//        if (count == 0) {
//            throw new BizException("PDK.DOWNLOAD.SOURCE.FAILED");
//        }
    fileService1.viewImg(id, response);
  }


    public List<TaskDto> findTaskById(String id, UserDetail user) {
        Criteria criteria = Criteria.where("syncType").is("sync").and("dag.nodes")
                .elemMatch(Criteria.where("type").is("custom_processor").and("customNodeId").is(id))
                .and("is_deleted").ne(true);

        Query query = new Query(criteria);
        query.fields().include("_id", "name", "status");

        List<TaskDto> taskDtos = taskService.findAllDto(query, user);
        return taskDtos;
    }

    public Map<String, CustomNodeDto> batchImport(List<CustomNodeDto> customNodeDtos, UserDetail user, boolean cover) {

        Map<String, CustomNodeDto> conMap = new HashMap<>();
        for (CustomNodeDto customNodeDto : customNodeDtos) {
            Query query = new Query(Criteria.where("_id").is(customNodeDto.getId()));
            query.fields().include("_id");
            CustomNodeDto customNode = findOne(query);
            if (customNode == null) {
                customNode = importEntity(customNodeDto, user);
            } else {
                if (cover) {
                    customNode = save(customNodeDto, user);
                }
            }

            conMap.put(customNodeDto.getId().toHexString(), customNode);

        }
        return conMap;
    }

    public CustomNodeDto importEntity(CustomNodeDto dto, UserDetail userDetail) {
        CustomNodeEntity entity = repository.importEntity(convertToEntity(CustomNodeEntity.class, dto), userDetail);
        return convertToDto(entity, CustomNodeDto.class);
    }
}


