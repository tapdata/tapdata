package com.tapdata.tm.discovery.bean;

import lombok.Data;

import java.util.List;

/**
 * 存储数据预览
 * */
@Data
public class DiscoveryStoragePreviewDto extends DiscoveryStorageDto {
    private List<Object> fields;
}
