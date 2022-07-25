import io.tapdata.common.constant.MqOp;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class Main {
    public static void main(String[] args) throws Throwable {
//        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory("tcp://192.168.1.183:61616");
//        activeMQConnectionFactory.setUserName("admin");
//        activeMQConnectionFactory.setPassword("admin");
//        ActiveMQConnection connection = (ActiveMQConnection) activeMQConnectionFactory.createConnection();
//        connection.start();
//        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//        Destination destination = session.createQueue("jarad_test");
//        MessageConsumer consumer = session.createConsumer(destination);
//        Message message = consumer.receive();
//        System.out.println(message);
//        session.close();
//        connection.close();
        MqOp.fromValue("insert");
    }
}
