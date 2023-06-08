package io.tapdata.connector.gauss.config;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Author:Skeet
 * Date: 2023/6/6
 **/
public class gaussJdbc {
    //以下代码将获取数据库连接操作封装为一个接口，可通过给定用户名和密码来连接数据库。
    public static Connection getConnect(String username, String passwd)
    {
        //驱动类。
        String driver = "com.huawei.opengauss.jdbc.Driver";
        //数据库连接描述符。
        String sourceURL = "jdbc:opengauss://159.138.3.20:8000/postgres";
        Connection conn = null;

        try
        {
            //加载驱动。
            Class.forName(driver);
        }
        catch( Exception e )
        {
            e.printStackTrace();
            return null;
        }

        try
        {
            //创建连接。
            conn = DriverManager.getConnection(sourceURL, username, passwd);
            System.out.println("Connection succeed!");
        }
        catch(Exception e)
        {
            e.printStackTrace();
            return null;
        }

        return conn;
    };

//    [create table "public".tapdata___test(col1 int not null, primary key(col1)), insert into "public".tapdata___test values(0), update "public".tapdata___test set col1=1 where 1=1, delete from "public".tapdata___test where 1=1, drop table "public".tapdata___test]

    public static void main(String[] args) {
        getConnect("root","Gotapd8!");
    }
}
