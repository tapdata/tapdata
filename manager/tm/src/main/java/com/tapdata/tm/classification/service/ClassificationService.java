package com.tapdata.tm.classification.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.classification.dto.ClassificationDto;
import com.tapdata.tm.classification.entity.ClassificationEntity;
import com.tapdata.tm.classification.repository.ResourceTagRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.bean.ResourceTagQueryDto;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @Author: Zed
 * @Date: 2021/8/20
 * @Description: 资源分类业务处理
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class ClassificationService extends BaseService<ClassificationDto, ClassificationEntity, ObjectId, ResourceTagRepository> {
    private DataSourceService dataSourceService;

    public ClassificationService(@NonNull ResourceTagRepository repository) {
        super(repository, ClassificationDto.class, ClassificationEntity.class);
    }

    /**
     * 修改资源分类名称
     *
     * @param user
     * @param id
     * @param newName
     * @return
     */

    public ClassificationDto rename(UserDetail user, String id, String newName) {
        //查询需要修改后的名称是否已经存在资源分类信息
        long count = findCountByName(newName);
        if (count > 0) {
            throw new BizException("Tag.RepeatName", "newName Resource tag is exist");
        }

        //判断分类是否属于当前用户
        ClassificationDto classificationDto = findById(new ObjectId(id), user);
        if (classificationDto == null) {
            throw new BizException("Tag.NotFound", "This resource tag is not belong to current user");
        }

        //修改分类信息
        Update update = Update.update("value", newName);
        updateById(new ObjectId(id), update, user);

        return findById(new ObjectId(id), user);
    }

    private long findCountByName(String name) {
        Query query = new Query();
        Criteria criteria = Criteria.where("value").is(name);
        query.addCriteria(criteria);
        return repository.count(query);
    }

    /**
     * 根据id删除数据源分类
     *
     * @param user
     * @param id
     * @return
     */

    public void delete(UserDetail user, String id) {
        //判断分类是否属于当前用户
        Optional<ClassificationEntity> optionalById = repository.findById(new ObjectId(id), user);
        if (!optionalById.isPresent()) {
            throw new BizException("Tag.NotFound", "This resource tag not found or not belong to current user");
        }

        //遍历得到当前便签的所有子标签
        List<ObjectId> childTags = getChildTags(optionalById.get().getId());

        //根据id删除分类及其分类的子分类
        for (ObjectId childTag : childTags) {
            deleteById(childTag, user);
        }

        //删除包含该标签的所有数据源连接中的该标签
        dataSourceService.deleteTags(childTags, user);
    }

    /**
     * 获取当前标签的子标签
     *
     * @return
     */
    public List<ObjectId> getChildTags(ObjectId id) {
        Query query = Query.query(Criteria.where("parentId").is(id));
        query.fields().include("_id");
        List<ClassificationEntity> childTags = repository.findAll(query);
        List<ObjectId> returnTagIds = childTags.stream().map(ClassificationEntity::getId).collect(Collectors.toList());
        for (ClassificationEntity classificationEntity : childTags) {
            List<ObjectId> childIds = getChildTags(classificationEntity.getId());
            returnTagIds.addAll(childIds);
        }
        return returnTagIds;
    }


    /**
     * 根据条件查询数资源分类
     *
     * @param user
     * @param queryDto
     * @return
     */

    public Page<ClassificationDto> list(UserDetail user, ResourceTagQueryDto queryDto) {
        //根据分类过滤信息与用户信息及其分页信息查询分类列表
        //根据名称过滤与分页信息，查询数据源类型列表并返还
        Filter filter = new Filter(queryDto.getPageNum(), queryDto.getPageSize(), new ArrayList<>());
        Where where = new Where();
        if (StringUtils.isNotBlank(queryDto.getName())) {
            where.and("name", queryDto.getName());
        }

        filter.setWhere(where);
        return find(filter, user);
    }


    protected void beforeSave(ClassificationDto tag, UserDetail user) {
        //根据名称判断是否已经存在资源分类信息
        long count = findCountByName(tag.getValue());
        if (count > 0) {
            throw new BizException("Tag.TagExist", "Resource tag is exist");
        }
    }
}
