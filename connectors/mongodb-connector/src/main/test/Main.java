import org.bson.types.Decimal128;

import java.math.BigDecimal;

public class Main {

    public static void main(String[] args) {
        String str = "1707897714.2126008954387257228239939";
        System.out.println(new BigDecimal(str).scale());
        new Decimal128(new BigDecimal(str));
    }
}
