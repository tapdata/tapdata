import java.time.ZoneId;
import java.util.Date;
import java.util.TimeZone;

public class MainDemo {

    public static void main(String[] args) {
        Date date = new Date(1659007942L);
//        System.out.println(date);

        TimeZone timeZone = TimeZone.getTimeZone(ZoneId.systemDefault());
        ZoneId of = ZoneId.of("Asia/Shanghai");
        TimeZone timeZone1 = TimeZone.getTimeZone(of);
        TimeZone timeZone2 = TimeZone.getTimeZone(ZoneId.of("Asia/Tokyo"));
        System.out.println(timeZone);
        String[] availableIDs = TimeZone.getAvailableIDs();
        for (String availableID : availableIDs) {
            System.out.println(availableID);
        }


    }
}
