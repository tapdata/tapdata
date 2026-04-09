package com.tapdata.tm.commons.schema;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Field;

import java.io.Serializable;
import java.util.Objects;

/**
 * @Author: Zed
 * @Date: 2021/9/2
 * @Description: connect内嵌tag文档
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Tag implements Serializable {

    /**
     * 分类id
     */
//    @JsonSerialize(using = ObjectIdSerialize.class)
//    @JsonDeserialize(using = ObjectIdDeserialize.class)
            @Field("id")
    private String id;
    /**
     * 分类名称
     */
    private String value;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tag tag = (Tag) o;
        return Objects.equals(id, tag.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
