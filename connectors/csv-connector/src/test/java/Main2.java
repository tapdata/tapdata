import com.opencsv.CSVWriter;
import io.tapdata.file.TapFileStorage;
import io.tapdata.file.TapFileStorageBuilder;
import io.tapdata.storage.s3fs.S3fsFileStorage;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

public class Main2 {
    public static void main(String[] args) throws Exception {
//        DateUtil.parse("1999-12-31 18:08:08.888888888");
//        String fileNameExpression = "jarad_${date:yyyyMM}_cache_h_${date:yyyy_MM_dd_HH:mm:ss}pg_${record.fuck}";
//        System.out.println(replaceDateSign(fileNameExpression));
//        OutputStream outputStream = new FileOutputStream("/Users/jarad/Desktop/aa.csv", true);
//        Writer writer = new OutputStreamWriter(outputStream);
//        CSVWriter csvWriter = new CSVWriter(writer);
//        csvWriter.writeNext(new String[]{"a", "s"});
//        csvWriter.writeNext(new String[]{"a", "s"});
//        csvWriter.writeNext(new String[]{"a", "s"});
//        csvWriter.writeNext(new String[]{"a", "s"});
//        csvWriter.flush();
//        csvWriter.writeNext(new String[]{"a", "s"});
//        csvWriter.writeNext(new String[]{"a", "s"});
//        csvWriter.writeNext(new String[]{"a", "s"});
//        csvWriter.writeNext(new String[]{"a", "s"});
//        csvWriter.flush();
//        csvWriter.close();
//        writer.close();
//        outputStream.close();
        TapFileStorage storage = new TapFileStorageBuilder()
                .withClassLoader(S3fsFileStorage.class.getClassLoader()) //PDK's classloader
                .withParams(map(
                        entry("endpoint", "127.0.0.1:9000"),
                        entry("accessKey", "k1nBAv1kKJwGRDJv"),
                        entry("secretKey", "1d3Ip6HWTIOKMeQLyGDwvQsaN2TZV3A0"),
                        entry("bucket", "jarad")
                ))
                .withStorageClassName("io.tapdata.storage.s3fs.S3fsFileStorage")
                .build();
    }

    private static String replaceDateSign(String fileNameExpression) {
        StringBuilder res = new StringBuilder();
        Date date = new Date();
        String subStr = fileNameExpression;
        while (subStr.contains("${date:")) {
            res.append(subStr, 0, subStr.indexOf("${date:"));
            subStr = subStr.substring(subStr.indexOf("${date:") + 7);
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(subStr.substring(0, subStr.indexOf("}")));
            res.append(simpleDateFormat.format(date));
            subStr = subStr.substring(subStr.indexOf("}") + 1);
        }
        res.append(subStr);
        return res.toString();
    }
}
