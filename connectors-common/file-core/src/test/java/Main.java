import java.io.File;
import java.util.Objects;

public class Main {
    public static void main(String[] args) {
        File file = new File("/Users/jarad");
        for(File str : Objects.requireNonNull(file.listFiles())) {
            System.out.println(str);
        }

        FileProtocolInterface service = new FtpFileService();
        service.listFiles();
    }
}
