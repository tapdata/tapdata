package io.tapdata.connector.empty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.time.ZoneId;
import java.util.TimeZone;

public class Main {
    public static void main(String[] args) throws Exception {
//        System.out.println(toTimeStr(-3599000));
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/COOLGJ?useCursorFetch=true", "root", "gj0628");
        Statement stm = connection.createStatement();
        stm.setFetchSize(10);
        ResultSet rs = stm.executeQuery("select * from `TestTime`");
        while(rs.next()) {
            System.out.println(rs.getString(2));
        }
        stm.close();
        connection.close();
    }

    public static String toTimeStr(long seconds) {
        DecimalFormat decimalFormat = new DecimalFormat("00");
        TimeZone.getDefault().getRawOffset();
        int hour;
        if (seconds >= 0) {
            hour = (int) (seconds / (60 * 60));
        } else {
            hour = (int) ((seconds + 1) / (60 * 60)) - 1;
            seconds = seconds - hour * 3600L;
        }
        int minute = (int) (seconds % (60 * 60) / 60);
        int second = (int) (seconds % 60);
        return decimalFormat.format(hour) + ":" + decimalFormat.format(minute) + ":" + decimalFormat.format(second);
    }
}
