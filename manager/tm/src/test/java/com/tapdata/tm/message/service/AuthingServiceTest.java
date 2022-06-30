package com.tapdata.tm.message.service;

import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class AuthingServiceTest extends BaseJunit {

    @Autowired
    UserService userService;

    @Test
    void getUserInfoByAuthing() {
        UserDto userDto=userService.findById(MongoUtils.toObjectId("61408608c4e5c40012663090"));
//        UserDetail userDetail=userService.loadUserById(MongoUtils.toObjectId("61408608c4e5c40012663090"));

//        authingService.getUserInfoByAuthing(userDetail.getUserId());
//        authingService.getUserInfoByAuthing(userDto.getExternalUserId());
    }




}