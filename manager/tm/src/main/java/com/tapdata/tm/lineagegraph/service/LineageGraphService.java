package com.tapdata.tm.lineagegraph.service;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.lineagegraph.dto.LineageGraphDto;
import com.tapdata.tm.lineagegraph.entity.LineageGraphEntity;
import com.tapdata.tm.lineagegraph.repository.LineageGraphRepository;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author:
 * @Date: 2021/10/15
 * @Description:
 */
@Service
@Slf4j
public class LineageGraphService extends BaseService<LineageGraphDto, LineageGraphEntity, ObjectId, LineageGraphRepository> {

    private static final String TABLE_LEVEL = "table";
    private static final String FIELD_LEVEL = "field";

    public LineageGraphService(@NonNull LineageGraphRepository repository) {
        super(repository, LineageGraphDto.class, LineageGraphEntity.class);
    }

    protected void beforeSave(LineageGraphDto lineageGraph, UserDetail user) {

    }

    public void graphData(String level, String qualifiedName, List<Field> fields) {
        if (StringUtils.isBlank(level)) {
            level = TABLE_LEVEL;
        }

        switch (level) {
            case TABLE_LEVEL:
                if (StringUtils.isBlank(qualifiedName)) {
                    throw new BizException("Invalid input param: qualifiedName cannot be empty");
                }
        }


    }
    public void refreshGraphData() {

    }
}