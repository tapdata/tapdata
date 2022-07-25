import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

public class Main {
    public static void main(String[] args) throws RemotingException, InterruptedException, MQClientException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setNamesrvAddr("192.168.1.126:9876");
        defaultMQAdminExt.start();
        defaultMQAdminExt.fetchAllTopicList();
        defaultMQAdminExt.shutdown();
        System.out.println();
    }
}
