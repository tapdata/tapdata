package io.tapdata.connector.mysql.util;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/11/18 17:17 Create
 */
public class MysqlBinlogPositionUtil implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(MysqlBinlogPositionUtil.class);

    private static final long CONNECTION_TIMEOUT = 60 * 1000;
    private static final long KEEP_ALIVE_INTERVAL = 60 * 1000;


    private final String filePrefix;
    private final int fileBegin;
    private final int fileEnd;
    private final BinaryLogClient client;

    public MysqlBinlogPositionUtil(String host, int port, String username, String password) throws SQLException {
        try (Connection conn = getConnection(host, port, username, password)) {
            String firstBinlogFilename = queryOneString(conn, "show binlog events limit 1");
            if (null == firstBinlogFilename) throw new RuntimeException("not found first binlog filename.");

            String lastBinlogFilename = queryOneString(conn, "show master status");
            if (null == lastBinlogFilename) throw new RuntimeException("not found first binlog filename.");

            this.fileBegin = Integer.parseInt(firstBinlogFilename.substring(firstBinlogFilename.indexOf(".") + 1));
            this.fileEnd = Integer.parseInt(lastBinlogFilename.substring(lastBinlogFilename.indexOf(".") + 1));
            this.filePrefix = lastBinlogFilename.substring(0, lastBinlogFilename.indexOf(".") + 1);
        }

        this.client = new BinaryLogClient(host, port, username, password);
        this.client.setServerId(MysqlUtil.randomServerId());
        this.client.setKeepAliveInterval(KEEP_ALIVE_INTERVAL);
        this.client.setHeartbeatInterval((long) (KEEP_ALIVE_INTERVAL * 0.8));
        this.client.setConnectTimeout(CONNECTION_TIMEOUT);
        this.client.setBlocking(false);
        this.client.registerLifecycleListener(new BinaryLogClient.LifecycleListener() {
            @Override
            public void onConnect(BinaryLogClient client) {
            }

            @Override
            public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
                logger.warn("Find MysqlBinlogPosition onCommunicationFailure: {}", ex.getMessage(), ex);
            }

            @Override
            public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
                logger.warn("Find MysqlBinlogPosition onEventDeserializationFailure: {}", ex.getMessage(), ex);
            }

            @Override
            public void onDisconnect(BinaryLogClient client) {
            }
        });
    }

    public MysqlBinlogPosition findByLessTimestamp(long ts, boolean useFirstIfPurged) throws IOException {
        logger.info(String.format("Find mysql binlog position in %s%06d~%06d", filePrefix, fileBegin, fileEnd));

        String lastFileName = null;
        MysqlBinlogPosition mysqlBinlogPosition;
        for (int num = fileEnd; num >= fileBegin; num--) {
            lastFileName = String.format("%s%06d", filePrefix, num);

            mysqlBinlogPosition = findByLessTimestampAndFilename(ts, lastFileName);
            if (null != mysqlBinlogPosition) {
                return mysqlBinlogPosition;
            }
        }

        if (useFirstIfPurged && null != lastFileName) {
            logger.info("Use first event binlog in {}/0", lastFileName);
            return new MysqlBinlogPosition(lastFileName, 0);
        }
        return null;
    }

    private MysqlBinlogPosition findByLessTimestampAndFilename(long ts, String filename) throws IOException {
        AtomicLong findPosition = new AtomicLong(-1);// -1: not found; -2: larger than ts; other: success
        AtomicLong lastPosition = new AtomicLong(0);

        client.setBinlogFilename(filename);
        client.registerEventListener(event -> {
            if (event.getHeader().getEventType() == EventType.HEARTBEAT) return;

            long eventTs = event.getHeader().getTimestamp();
            if (eventTs != 0) {
                if (eventTs <= ts) {
                    if (-1 == findPosition.get()) {
                        logger.info("Check MysqlBinlogPosition in {}, first event: {}", filename, event);
                    }
                    findPosition.set(lastPosition.get());
                } else {
                    if (findPosition.compareAndSet(-1, -2)) {
                        logger.info("Not found MysqlBinlogPosition: {}/{}", filename, client.getBinlogPosition());
                    }
                    try {
                        client.disconnect();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return;
                }
            }

            lastPosition.set(client.getBinlogPosition());
        });

        try {
            client.connect();
        } finally {
            client.disconnect();
        }

        if (0 <= findPosition.get()) {
            return new MysqlBinlogPosition(filename, findPosition.get());
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        client.disconnect();
    }

    private static Connection getConnection(String host, int port, String username, String password) throws SQLException {
        String jdbcUrl = String.format("jdbc:mysql://%s:%d", host, port);
        Driver driver = DriverManager.getDriver(jdbcUrl);

        Properties props = new Properties();
        Optional.ofNullable(username).ifPresent(s -> props.put("user", s));
        Optional.ofNullable(password).ifPresent(s -> props.put("password", s));

        logger.debug("connect: {}", jdbcUrl);
        return driver.connect(jdbcUrl, props);
    }

    private static String queryOneString(Connection conn, String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            try (ResultSet rs = s.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        Date beginDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2022-11-15 00:00:00");
        long beginTimes = beginDate.getTime();

        try (MysqlBinlogPositionUtil finder = new MysqlBinlogPositionUtil("localhost", 6002, "root", "Gotapd8!")) {
            MysqlBinlogPosition mysqlBinlogPosition = finder.findByLessTimestamp(beginTimes, true);
            if (null == mysqlBinlogPosition) {
                System.out.println("Not found MysqlBinlogPosition");
            } else {
                System.out.println(mysqlBinlogPosition.getFilename() + "/" + mysqlBinlogPosition.getPosition());
            }
        }
    }

}
