package com.tapdata.tm.userLog.service;

import cn.hutool.core.util.RandomUtil;
import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.entity.Connected;
import com.tapdata.tm.user.entity.Notification;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.types.ObjectId;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Base64;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class UserServiceTest extends BaseJunit {

    @Autowired
    UserService userService;


    @Test
    void findAll() {
    }

    @Test
    void findById2() {
        UserDto userDto = userService.findById(MongoUtils.toObjectId("62172cfc49b865ee5379d3ed"));
        Notification notification = userDto.getNotification();
        printResult(notification);
    }

    @Test
    void add() {
    }

    @Test
    void updateById() {
        User user = new User();
        user.setId(new ObjectId("613f37e5a703840012b36d15"));
        Notification notification = new Notification();

        Connected connected = new Connected();
        connected.setEmail(false);

        notification.setConnected(connected);
        user.setNotification(notification);
        userService.updateById(user);
    }


    @Test
    void findPage() throws JSONException {
        String json = "{\"notification\":{\"connected\":{\"email\":true,\"sms\":false},\"connectionInterrupted\":{\"email\":true,\"sms\":true},\"stoppedByError\":{\"email\":true,\"sms\":false}}}";
//        userService.updateUserNotification("60fe642ea521f00012685a57", json);


        JSONObject jsonObject = new JSONObject(json);
        Iterator<String> iterator = jsonObject.keys();
        while (iterator.hasNext()) {
            String key = iterator.next();
            String value = jsonObject.getString(key);
            System.out.println("key: " + key + ",value:" + value);
        }

    }

    @Test
    public void decode() {

        String userOptions = new String(Base64.getDecoder().decode("Y3VzdG9tSWQ9NjEzZjM3ZGJiMDQzYjgzNTBhNjY4ZjRkJnVzZXJJZD02MTNmMzdkYmIwNDNiODM1MGE2NjhmNGQmYWRtaW49MCZ1c2VybmFtZT0xODAwMjU2OTEwOSZjdXN0b21lclR5cGU9bnVsbA=="));
        System.out.println(userOptions);
    }


    @Test
    public void encode() {
        //customId=613f37dbb043b8350a668f4d&userId=613f37dbb043b8350a668f4d&admin=0&username=18002569109&customerType=null
        String userOptions = new String(Base64.getEncoder().encode("customId=613f37dbb043b8350a668f4d&userId=604f4b7ce1ca905fa754520c&admin=0&username=18002569109&customerType=null".getBytes()));
        System.out.println(userOptions);
    }

    @Test
    public void updateUserSetting() {
        String json = "{\"notification\":{\"connected\":{\"email\":true,\"sms\":true},\"connectionInterrupted\":{\"email\":true,\"sms\":true},\"stoppedByError\":{\"email\":true,\"sms\":false}}}";
       String json2="{\"guideData\":{\"noShow\":true,\"updateTime\":1632380823831,\"action\":true}}";

        userService.updateUserSetting("604f4b7ce1ca905fa754520c", json2);
    }






    /**
     * const S4 = function () {
     * return (((1 + Math.random()) * 0x10000) | 0).toString(16).substring(1)
     * };
     * const NewGuid = function () {
     * return (S4() + S4() + "" + S4() + "" + S4() + "" + S4() + "" + S4() + S4() + S4())
     * };
     *
     * @return
     */



    @Test
    public void  randomCode() {

        String validateCode= RandomUtil.randomNumbers(6);
        System.out.println(validateCode);
    }

    @Test
    public void  sendValidateCde() {

        userService.sendValidateCde("314445417@qq.com");
    }

    @Test
    public void findById() {
        printResult(userService.findById(new ObjectId("6050575762ed301e55add7fb")));
    }

}