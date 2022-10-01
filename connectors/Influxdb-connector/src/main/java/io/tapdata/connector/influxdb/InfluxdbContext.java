package io.tapdata.connector.influxdb;

import com.sun.net.httpserver.HttpServer;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import lombok.Getter;
import lombok.Setter;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.SQLException;

@Setter
@Getter
public class InfluxdbContext implements AutoCloseable{
    
    private static final String TAG = InfluxdbContext.class.getSimpleName();
    
    private TapConnectionContext tapConnectionContext;
    private InfluxdbConfig influxDBConfig;
    private transient InfluxDB influxDBClient;
    
    //Http连接Influxdb
    private HttpServer server;
    
    public InfluxdbContext(TapConnectionContext tapConnectionContext){
        this.tapConnectionContext = tapConnectionContext;
        DataMap config = tapConnectionContext.getConnectionConfig();
        if (influxDBConfig == null){
            influxDBConfig = new InfluxdbConfig().load(config);
        }
        
        //TODO 实现连接influxdb
        int defaultPort = influxDBConfig.getPort();
        if (this.server != null) {
            return;
        }
        try {
            this.server = HttpServer.create(new InetSocketAddress(defaultPort), 0);
        } catch (final IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Unable to start HTTP Server on Port %d: %s",
                            defaultPort, e.getMessage()));
        }
        
        server.start();
        final String serverURL = "http://127.0.0.1:" + defaultPort;
        influxDBClient = InfluxDBFactory.connect(serverURL, influxDBConfig.getUser(), influxDBConfig.getPassword());
    
        if (!influxDBClient.databaseExists(influxDBConfig.getDatabase())) {
            if(influxDBConfig.isCreateDatabase()) {
                influxDBClient.createDatabase(influxDBConfig.getDatabase());
            }
            else {
                throw new RuntimeException("This " + influxDBConfig.getDatabase() + " database does not exist!");
            }
        }
    
        influxDBClient.setDatabase(influxDBConfig.getDatabase());
    
        if (influxDBConfig.getBatchActions() > 0) {
            influxDBClient.enableBatch(influxDBConfig.getBatchActions(), influxDBConfig.getFlushDuration(), influxDBConfig.getFlushDurationTimeUnit());
        }
    
        if (influxDBConfig.isEnableGzip()) {
        
            influxDBClient.enableGzip();
        }
    }
    
    public QueryResult executeQuery(String sql){
        return influxDBClient.query(new Query(sql, influxDBConfig.getDatabase()));
    }
    
    public void execute(String sql) throws SQLException {
        TapLogger.debug(TAG, "Execute sql: " + sql);
        influxDBClient.query(new Query(sql, influxDBConfig.getDatabase()));
    }
    
    
    @Override
    public void close() throws Exception {
        server.stop(1);
    }
    
    public InfluxDB getConnection() {
        return influxDBClient;
    }
    
    public InfluxdbConfig getInfluxDBConfig(){
        return influxDBConfig;
    }
}
