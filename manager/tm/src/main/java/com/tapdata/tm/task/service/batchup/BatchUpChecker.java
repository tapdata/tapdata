package com.tapdata.tm.task.service.batchup;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;
/**
 * @author Gavin'Xiao
 * @github https://github.com/11000100111010101100111
 * @Document
 * 86迭代开始，自动注册数据源只注册GA数据源，导入任务后存在数据源没注册或者版本不一致的情况
 *  - 需要在导入前检查数据源是否注册，没注册时需要注册相应数据源后再重新导入
 *  - 导入任务关联的数据源和已注册的数据源版本是否一致，
 *       - 使用的数据源从高版本导入到低版本环境，需要升级对应数据源后再导入
 *       - 使用的数据源从低版本导入到高版本，数据源可能存在差异，需要检查源连接的连接配置项
 * */
@Slf4j
@Service
@Setter(onMethod_ = {@Autowired})
public class BatchUpChecker {
    private DataSourceDefinitionService dataSourceDefinitionService;
    MetadataInstancesService metadataInstancesService;
    DataSourceService dataSourceService;

    public void checkDataSourceConnection(List<DataSourceConnectionDto> connections, List<MetadataInstancesDto> metadataInstances, UserDetail user) {
        if (CollectionUtils.isEmpty(connections)) {
            return;
        }
        for (DataSourceConnectionDto connection : connections) {
            String databaseType = connection.getDatabase_type();
            String definitionPdkAPIVersion = connection.getDefinitionPdkAPIVersion();

            Criteria userCriteria = new Criteria();
            userCriteria.and("customId").is(user.getCustomerId());
            Criteria supplierCriteria = Criteria.where("supplierType").ne("self");
            Criteria criteria = Criteria.where("type").in(databaseType);
            criteria.orOperator(userCriteria, supplierCriteria);

            List<DataSourceDefinitionDto> all = dataSourceDefinitionService.findAll(Query.query(criteria));

            //没有这个数据源，报错提示需要注册才能导入
            if (CollectionUtils.isEmpty(all)) {
                throw new BizException("task.import.connection.check.ConnectorNotRegister", databaseType, definitionPdkAPIVersion);
            }

            if (StringUtils.isBlank(definitionPdkAPIVersion)) {
                //@todo
                log.warn("");
                continue;
            }

            List<DataSourceDefinitionDto> sortList = all.stream()
                    .sorted(this::sortByPdkApiVersion)
                    .collect(Collectors.toList());
            DataSourceDefinitionDto upperOne = sortList.get(0);
            String pdkId = upperOne.getPdkId();

            String upperPdkAPIVersion = upperOne.getPdkAPIVersion();
            if (definitionPdkAPIVersion.equals(upperPdkAPIVersion)) {
                connection.setDefinitionPdkId(pdkId);
                //@todo
                log.info("");
                continue;
            }

            //导入任务中使用的数据源版本太低，告警提示版本会存在不兼容
            if (Check.LESS.equals(checkConnectionVersion(definitionPdkAPIVersion, upperPdkAPIVersion))) {
                connection.setDefinitionPdkId(pdkId);
                //@todo log.warn(导入任务中使用的数据源版本太低);
                log.warn("");
                continue;
            }

            //导入任务中使用的数据源版本太高，报错提示需要注册高版本才能导入
            DataSourceDefinitionDto lowerOne = sortList.get(sortList.size() - 1);
            String lowerPdkAPIVersion = lowerOne.getPdkAPIVersion();
            if (Check.MORE.equals(checkConnectionVersion(definitionPdkAPIVersion, lowerPdkAPIVersion))) {
                //导入任务中使用的数据源版本太高
                throw new BizException("task.import.connection.check.ConnectorVersionTooHeight", databaseType, lowerPdkAPIVersion, definitionPdkAPIVersion, databaseType);
            }
        }
    }

    protected int sortByPdkApiVersion(DataSourceDefinitionDto p1, DataSourceDefinitionDto p2) {
        Check check = checkConnectionVersion(p1.getPdkAPIVersion(), p2.getPdkAPIVersion());
        switch (check) {
            case LESS: return Check.LESS.status;
            case MORE: return Check.MORE.status;
            default: return Check.EQUALS.status;
        }
    }

    protected Check checkConnectionVersion(String definitionPdkAPIVersion, String checker) {
        int pdkBuildNumber = CommonUtils.getPdkBuildNumer(definitionPdkAPIVersion);
        int checkerPdkBuildNumber = CommonUtils.getPdkBuildNumer(checker);
        if (pdkBuildNumber > checkerPdkBuildNumber) {
            return Check.MORE;
        }
        return pdkBuildNumber == checkerPdkBuildNumber ? Check.EQUALS : Check.LESS;
    }

    enum Check {
        MORE(1),
        LESS(-1),
        EQUALS(0);
        int status;
        Check(int status) {
            this.status = status;
        }
        public boolean equals(Check c) {
            return null != c && c.status == this.status;
        }
    }
}
