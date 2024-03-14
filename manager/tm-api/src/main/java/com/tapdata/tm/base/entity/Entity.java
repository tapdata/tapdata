package com.tapdata.tm.base.entity;

import lombok.*;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;

import java.io.Serializable;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/4/19 下午3:20
 * @description
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class Entity implements Serializable {

    @Id
    @Field("_id")
    private ObjectId id;
}
