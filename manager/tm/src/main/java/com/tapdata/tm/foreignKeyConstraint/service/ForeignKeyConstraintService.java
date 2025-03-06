package com.tapdata.tm.foreignKeyConstraint.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.util.ThrowableUtils;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.foreignKeyConstraint.dto.ForeignKeyConstraintDto;
import com.tapdata.tm.foreignKeyConstraint.entity.ForeignKeyConstraintEntity;
import com.tapdata.tm.foreignKeyConstraint.repository.ForeignKeyConstraintRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletResponse;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

@Service
@Slf4j
public class ForeignKeyConstraintService extends BaseService<ForeignKeyConstraintDto, ForeignKeyConstraintEntity, ObjectId, ForeignKeyConstraintRepository> {
    public ForeignKeyConstraintService(ForeignKeyConstraintRepository repository) {
        super(repository, ForeignKeyConstraintDto.class, ForeignKeyConstraintEntity.class);
    }

    @Override
    protected void beforeSave(ForeignKeyConstraintDto dto, UserDetail userDetail) {

    }

    public void loadSqlFile(String taskId, HttpServletResponse response) {
        ForeignKeyConstraintDto foreignKeyConstraint = findOne(Query.query(Criteria.where("taskId").is(taskId)));
        try (OutputStream out = response.getOutputStream()){
            String fileName = taskId + "_" + "ForeignKeyConstraint" +".sql";
            String codeFileName = URLEncoder.encode(fileName, "UTF-8");

            response.setHeader("Content-disposition", "inline; filename=" + codeFileName);
            response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
            int length = 0;
            if(null != foreignKeyConstraint && CollectionUtils.isNotEmpty(foreignKeyConstraint.getSqlList())){
                for(String sql : foreignKeyConstraint.getSqlList()){
                    byte[] bytes = sql.getBytes(StandardCharsets.UTF_8);
                    out.write(bytes);
                    out.write("\n".getBytes(StandardCharsets.UTF_8));
                    length += bytes.length + 1;

                }
            }else{
                byte[] bytes = "No foreign key constraints that need to be imported".getBytes(StandardCharsets.UTF_8);
                out.write(bytes);
                length += bytes.length;
            }
            response.setContentLength(length);
            out.flush();
        } catch (Exception e) {
            log.error("loadSqlFile error {}", ThrowableUtils.getStackTraceByPn(e));
        }

    }


}
