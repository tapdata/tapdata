package com.tapdata.tm.ds.dto;

import com.tapdata.tm.commons.schema.Tag;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * @Author: Zed
 * @Date: 2021/9/23
 * @Description:
 */
@AllArgsConstructor
@Getter
@Setter
public class UpdateTagsDto {
    private List<String> id;
    private List<Tag> listtags;
}
