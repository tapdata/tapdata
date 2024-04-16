package com.tapdata.tm.agent.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
@Document("AgentGroup")
public class AgentGroupEntity extends BaseEntity {
    String groupId;
    String name;
    List<String> agentIds;
}
