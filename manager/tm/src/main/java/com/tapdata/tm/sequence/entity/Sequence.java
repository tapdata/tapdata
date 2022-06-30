package com.tapdata.tm.sequence.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.*;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/4/21 下午2:32
 * @description
 */
@Document("DrsSequence")
@Data
@EqualsAndHashCode(callSuper=false)
public class Sequence extends BaseEntity {

    /**
     * 分类
     */
    private String field;

    /**
     * 当前序列值
     */
    private long seq;

    /**
     * 初始值
     */
    private long begin;

    /**
     * 过期时间
     */
    private Date expire;
}
