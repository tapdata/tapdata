import java.util.regex.Matcher;

public class Main {
    public static void main(String[] args) {
        String str = "GGG${id}DDD";
        System.out.println(str.contains("${id}"));
    }
}
