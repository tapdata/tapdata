package com.tapdata.tm.discovery.bean;

import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import lombok.Data;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.List;

@Data
public class UnionQueryResult {
    private Date createTime;
    private ObjectId _id;
    private List listtags;
    private String meta_type;
    private String original_name;
    private String syncType;
    private String name;
    private String agentId;
    private SourceDto source;
    private String type;
    private String apiType;
    private String tableName;
    private String comment;
    private String desc;
    private String sourceInfo;
}
