package com.tapdata.tm.commons.externalStorage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExternalStorageDtoTest {
    @Test
    void maskUriPasswordTest(){
        ExternalStorageDto externalStorageDto = new ExternalStorageDto();
        externalStorageDto.setType(ExternalStorageType.mongodb.name());
        externalStorageDto.setUri("mongodb://test:test==@localhost:27017/test");
        Assertions.assertTrue(externalStorageDto.maskUriPassword().contains(ExternalStorageDto.MASK_PWD));
    }

    @Test
    void maskUriPasswordTest_uriHasEscapeCharacter(){
        ExternalStorageDto externalStorageDto = new ExternalStorageDto();
        externalStorageDto.setType(ExternalStorageType.mongodb.name());
        externalStorageDto.setUri("mongodb://test:test%3D%3D@localhost:27017/test");
        Assertions.assertTrue(externalStorageDto.maskUriPassword().contains(ExternalStorageDto.MASK_PWD));
    }

    @Test
    void maskUriPasswordTest_withoutType(){
        ExternalStorageDto externalStorageDto = new ExternalStorageDto();
        externalStorageDto.setUri("mongodb://test:test==@localhost:27017/test");
        Assertions.assertTrue(externalStorageDto.maskUriPassword().contains(ExternalStorageDto.MASK_PWD));
    }

    @Test
    void maskUriPasswordTest_srvUriWithoutType(){
        ExternalStorageDto externalStorageDto = new ExternalStorageDto();
        externalStorageDto.setUri("mongodb+srv://test:test%3D%3D@cluster0.example.com/test");
        Assertions.assertTrue(externalStorageDto.maskUriPassword().contains(ExternalStorageDto.MASK_PWD));
    }

    @Test
    void maskSensitiveDataTest(){
        ExternalStorageDto externalStorageDto = new ExternalStorageDto();
        externalStorageDto.setType(ExternalStorageType.mongodb.name());
        externalStorageDto.setUri("mongodb://test:test==@localhost:27017/test");
        externalStorageDto.setSslCA("ca-content");
        externalStorageDto.setSslKey("key-content");
        externalStorageDto.setSslPass("pass-content");
        externalStorageDto.setAccessToken("token-content");

        ExternalStorageDto masked = externalStorageDto.maskSensitiveData();

        Assertions.assertEquals(ExternalStorageDto.MASK_PWD, masked.getSslCA());
        Assertions.assertEquals(ExternalStorageDto.MASK_PWD, masked.getSslKey());
        Assertions.assertEquals(ExternalStorageDto.MASK_PWD, masked.getSslPass());
        Assertions.assertEquals(ExternalStorageDto.MASK_PWD, masked.getAccessToken());
        Assertions.assertTrue(masked.getUri().contains(ExternalStorageDto.MASK_PWD));
    }
}
