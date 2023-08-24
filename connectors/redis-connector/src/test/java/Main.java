import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueString;

import java.io.IOException;
import java.net.URISyntaxException;

public class Main {
    public static void main(String[] args) throws URISyntaxException, IOException {
        Replicator replicator = new RedisReplicator("redis://:gj0628@127.0.0.1:6379");
        replicator.addEventListener((replicator1, event) -> {
            if (event instanceof KeyStringValueString) {
                KeyStringValueString kv = (KeyStringValueString) event;
                System.out.println(new String(kv.getKey()));
                System.out.println(new String(kv.getValue()));
            } else {
            }
        });
        replicator.open();
    }
}
