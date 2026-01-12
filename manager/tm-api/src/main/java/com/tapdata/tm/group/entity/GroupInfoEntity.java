package com.tapdata.tm.group.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;
import com.tapdata.tm.group.dto.ResourceItem;

import java.util.ArrayList;
import java.util.List;

@Document("GroupInfo")
@Data
@EqualsAndHashCode(callSuper=false)
public class GroupInfoEntity extends BaseEntity {
    private String name;
    private String description;
    private List<ResourceItem> resourceItemList = new ArrayList<>();
}
