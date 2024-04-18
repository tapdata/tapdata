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
}
