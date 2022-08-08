package com.tapdata.tm.utils;

import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.BaseJunit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;
import java.util.Date;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

@Slf4j
class MailUtilsTest extends BaseJunit {

    @Autowired
    MailUtils mailUtils;

    @Test
    void send() {
    }

    @Test
    void sendHtml() {
    }

    /**
     * 读取html文件为String
     *
     * @param htmlFileName
     * @return
     * @throws Exception
     */
    public static String readHtmlToString(String htmlFileName) {
        InputStream is = null;
        Reader reader = null;
        try {
            is = MailUtils.class.getClassLoader().getResourceAsStream(htmlFileName);
            if (is == null) {
                log.error("未找到模板文件");
            }
            reader = new InputStreamReader(is, "UTF-8");
            StringBuilder sb = new StringBuilder();
            int bufferSize = 1024;
            char[] buffer = new char[bufferSize];
            int length = 0;
            while ((length = reader.read(buffer, 0, bufferSize)) != -1) {
                sb.append(buffer, 0, length);
            }
            return sb.toString();
        } catch (UnsupportedEncodingException e) {
            log.error("发送邮件异常", e);
        } catch (IOException e) {
            log.error("发送邮件异常", e);
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (IOException e) {
                log.error("关闭io流异常", e);
            }
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                log.error("关闭io流异常", e);
            }
        }
        return "";
    }

    @Test
    public void min5Later(){
        System.out.println((DateUtil.offsetMinute(new Date(),10)));
    }

    @Test
    public void sendValidateCode(){
    }

    @Test
    public void sendMail(){
        SendStatus sendStatus=new SendStatus();
        String html = readHtmlToString("mailTemplate.html");

        // 写入模板内容
        Document doc = Jsoup.parse(html);
        mailUtils.sendMail("admin@admin.com",sendStatus,doc,"da");
    }
}