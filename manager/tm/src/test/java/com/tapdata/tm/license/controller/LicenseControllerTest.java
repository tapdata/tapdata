package com.tapdata.tm.license.controller;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LicenseControllerTest {

    @Test
    public void test() {
        String id = "[\"e33cfc413dd89ad28ca433d91782929e79521ca6b5d88ee633cd50d69aeda6a0\"]";
        JSONArray ids = JSONUtil.parseArray(id);
        System.out.println(ids.toString());
    }

}