package com.tapdata.tm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.ds.entity.DataSourceEntity;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class MockJsonUtils {
    public static final String MOCK_JSON_DIR= "mockjsonfile/metadataInstance";
    public static <T> T getDtoFromJsonFile(String dir, String fileName, Class<T> valueType) {
        String pathInResources = dir + File.separator + fileName;
        URL metadataInstanceJsonFileURL = MockJsonUtils.class.getClassLoader().getResource(pathInResources);
        if (null == metadataInstanceJsonFileURL) {
            throw new RuntimeException(String.format("Cannot get url: '%s', check your json file name and path is correct", pathInResources));
        }
        ObjectMapper objectMapper=new ObjectMapper();
        try {
            return objectMapper.readValue(metadataInstanceJsonFileURL, valueType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        MetadataInstancesDto metadataInstancesDto = getDtoFromJsonFile(MOCK_JSON_DIR,"metadataInstancesDto.json", MetadataInstancesDto.class);
        String connectionsDir="mockjsonfile/connections";
        DataSourceConnectionDto dtoFromJsonFile = getDtoFromJsonFile(connectionsDir, "mysqljson.json", DataSourceConnectionDto.class);
        System.out.println(dtoFromJsonFile);
    }
}
