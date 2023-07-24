package com.gbase8s.stream.cdc;

import com.gbasedbt.jdbc.IfxSmartBlob;
import com.informix.stream.impl.IfxStreamException;

import java.sql.*;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Author:Skeet
 * Date: 2023/6/26
 **/
public class Gbase8sCdc implements AutoCloseable {
    private Connection con;
    private int sessionID;
    private boolean isClosed;
    private boolean stopLoggingOnClose = true;
    private IfxSmartBlob smartBlob;
    private final IfxCDCRecordBuilder recordBuilder;
    private List<IfmxWatchedTable> capturedTables;


    public Gbase8sCdc(Connection con) {
        this.con = con;
        this.recordBuilder = new IfxCDCRecordBuilder(con);
    }

    public void init() throws SQLException, IfxStreamException {
        Statement s = this.con.createStatement();
        Throwable var3 = null;
        String serverName;
        ResultSet rs = null;
        Throwable var5;
        try {
            rs = s.executeQuery("SELECT env_value FROM sysmaster:sysenv where \"env_value\" = 'gbase01'");
            var5 = null;

            try {
                rs.next();
                serverName = rs.getString(1).trim();
            } catch (Throwable var77) {
                var5 = var77;
                throw var77;
            } finally {
                if (rs != null) {
                    if (var5 != null) {
                        try {
                            rs.close();
                        } catch (SQLException var76) {
                            var5.addSuppressed(var76);
                        }
                    } else {
                        rs.close();
                    }
                }
            }
        } catch (Throwable var79) {
            var3 = var79;
            throw var79;
        } finally {
            if (s != null) {
                if (var3 != null) {
                    try {
                        s.close();
                    } catch (Throwable var75) {
                        var3.addSuppressed(var75);
                    }
                } else {
                    s.close();
                }
            }
        }

        CallableStatement cstmt = this.con.prepareCall("EXECUTE FUNCTION cdc_opensess(?,?,?,?,?,?)");
        var3 = null;

        try {
            cstmt.setString(1, serverName);
            cstmt.setInt(2, 0);
            cstmt.setInt(3, 5);//timeout
            cstmt.setInt(4, 255);//fetchSize
            cstmt.setInt(5, 1);
            cstmt.setInt(6, 0);
            rs = cstmt.executeQuery();
            var5 = null;

            try {
                rs.next();
                this.sessionID = rs.getInt(1);
                if (this.sessionID < 0) {
                    throw new IfxStreamException("Unable to create CDC session. Error code: " + this.sessionID);
                }
            } catch (Throwable var81) {
                var5 = var81;
                throw var81;
            } finally {
                if (rs != null) {
                    if (var5 != null) {
                        try {
                            rs.close();
                        } catch (Throwable var74) {
                            var5.addSuppressed(var74);
                        }
                    } else {
                        rs.close();
                    }
                }

            }
        } catch (Throwable var83) {
            var3 = var83;
            throw var83;
        } finally {
            if (cstmt != null) {
                if (var3 != null) {
                    try {
                        cstmt.close();
                    } catch (Throwable var73) {
                        var3.addSuppressed(var73);
                    }
                } else {
                    cstmt.close();
                }
            }
        }

        this.smartBlob = new IfxSmartBlob(this.con);
        Iterator var86 = this.capturedTables.iterator();

        while (var86.hasNext()) {
            IfmxWatchedTable table = (IfmxWatchedTable) var86.next();
            this.watchTable(table);
        }

        this.activateSession();
    }

    private void watchTable(IfmxWatchedTable table) throws SQLException, IfxStreamException {
        this.setFullRowLogging(table.getDesciptorString(), true);
        this.startCapture(table);
    }

    private void setFullRowLogging(String tableName, boolean enable) throws IfxStreamException {

        try {
            CallableStatement cstmt = this.con.prepareCall("EXECUTE FUNCTION cdc_set_fullrowlogging(?,?);");
            Throwable var4 = null;

            cstmt.setString(1, tableName);
            if (enable) {
                cstmt.setInt(2, 1);
            } else {
                cstmt.setInt(2, 0);
            }

            ResultSet rs = cstmt.executeQuery();
            Throwable var6 = null;

            try {
                rs.next();
                int resultCode = rs.getInt(1);
                if (resultCode != 0) {
                    throw new IfxStreamException("Unable to set full row logging. Error code: " + resultCode);
                }
            } catch (SQLException var31) {
                var6 = var31;
                throw var31;
            } finally {
                if (cstmt != null) {
                    if (var4 != null) {
                        try {
                            cstmt.close();
                        } catch (Throwable var29) {
                            var4.addSuppressed(var29);
                        }
                    } else {
                        cstmt.close();
                    }
                }
            }
        } catch (SQLException var35) {
            throw new IfxStreamException("Unable to set full row logging ", var35);

        }
    }

    private void startCapture(IfmxWatchedTable table) throws SQLException {
        Throwable var3;
        ResultSet rs;
        Throwable var5;
        if (table.getColumnDescriptorString().equals("*")) {
            Statement s = this.con.createStatement(1003, 1007);
            var3 = null;

            try {
                rs = s.executeQuery("SELECT FIRST 1 * FROM" + table.getDesciptorString());
                var5 = null;
                try {
                    ResultSetMetaData md = rs.getMetaData();
                    String[] columns = new String[md.getColumnCount()];

                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        columns[i - 1] = md.getColumnName(i).trim();
                    }

                    table.columns(columns);
                } catch (SQLException var88) {
                    var5 = var88;
                    throw var88;
                } finally {
                    if (rs != null) {
                        if (var5 != null) {
                            try {
                                rs.close();
                            } catch (Throwable var80) {
                                var5.addSuppressed(var80);
                            }
                        } else {
                            rs.close();
                        }
                    }
                }
            } catch (Throwable var90) {
                var3 = var90;
                throw var90;
            } finally {
                if (s != null) {
                    if (var3 != null) {
                        try {
                            s.close();
                        } catch (Throwable var79) {
                            var3.addSuppressed(var79);
                        }
                    } else {
                        s.close();
                    }
                }
            }
        }

        try {
            CallableStatement cstmt = this.con.prepareCall("EXECUTE FUNCTION cdc_startcapture(?,?,?,?,?)");
            var3 = null;

            try {
                cstmt.setInt(1, this.sessionID);
                cstmt.setLong(2, 0L);
                cstmt.setString(3, table.getDesciptorString());
                cstmt.setString(4, table.getColumnDescriptorString());
                cstmt.setInt(5, table.getLabel());
                rs = cstmt.executeQuery();
                var5 = null;

                try {
                    rs.next();
                    int resultCode = rs.getInt(1);
                    if (resultCode != 0) {
                        throw new SQLException("CDCConnection: Unable to start cdc capture. Error code: " + resultCode);
                    }
                } catch (Throwable var83) {
                    var5 = var83;
                    throw var83;
                } finally {
                    if (rs != null) {
                        if (var5 != null) {
                            try {
                                rs.close();
                            } catch (Throwable var82) {
                                var5.addSuppressed(var82);
                            }
                        } else {
                            rs.close();
                        }
                    }
                }
            } catch (Throwable var85) {
                var3 = var85;
                throw var85;
            } finally {
                if (cstmt != null) {
                    if (var3 != null) {
                        try {
                            cstmt.close();
                        } catch (Throwable var81) {
                            var3.addSuppressed(var81);
                        }
                    } else {
                        cstmt.close();
                    }
                }
            }
        } catch (SQLException var87) {
            throw new SQLException("CDCConnection: Unable to start cdc capture ", var87);
        }
    }

    private void activateSession() throws SQLException, IfxStreamException {
        try {
            CallableStatement cstmt = this.con.prepareCall("execute function cdc_activatesess(?,?)");
            Throwable var2 = null;

            try {
                cstmt.setInt(1, this.sessionID);
                cstmt.setLong(2, 0);
                ResultSet rs = cstmt.executeQuery();
                Throwable var4 = null;

                try {
                    rs.next();
                    int resultCode = rs.getInt(1);
                    if (resultCode != 0) {
                        throw new IfxStreamException("Unable to activate session. Error code: " + resultCode);
                    }
                } catch (Throwable var29) {
                    var4 = var29;
                    throw var29;
                } finally {
                    if (rs != null) {
                        if (var4 != null) {
                            try {
                                rs.close();
                            } catch (Throwable var28) {
                                var4.addSuppressed(var28);
                            }
                        } else {
                            rs.close();
                        }
                    }
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            } catch (Throwable var31) {
                var2 = var31;
                throw var31;
            } finally {
                if (cstmt != null) {
                    if (var2 != null) {
                        try {
                            cstmt.close();
                        } catch (Throwable var27) {
                            var2.addSuppressed(var27);
                        }
                    } else {
                        cstmt.close();
                    }
                }
            }
        } catch (SQLException var33) {
            throw new IfxStreamException("Unable to activate session", var33);
        }
    }

    public void closeSession() throws IfxStreamException {

        try {
            CallableStatement cstmt = this.con.prepareCall("execute function informix.cdc_closesess(?)");
            Throwable var2 = null;

            try {
                cstmt.setInt(1, this.sessionID);
                ResultSet rs = cstmt.executeQuery();
                Throwable var4 = null;

                try {
                    rs.next();
                    int resultCode = rs.getInt(1);
                    if (resultCode != 0) {
                        throw new IfxStreamException("Unable to close session. Error code: " + resultCode);
                    }
                } catch (Throwable var29) {
                    var4 = var29;
                    throw var29;
                } finally {
                    if (rs != null) {
                        if (var4 != null) {
                            try {
                                rs.close();
                            } catch (Throwable var28) {
                                var4.addSuppressed(var28);
                            }
                        } else {
                            rs.close();
                        }
                    }

                }
            } catch (Throwable var31) {
                var2 = var31;
                throw var31;
            } finally {
                if (cstmt != null) {
                    if (var2 != null) {
                        try {
                            cstmt.close();
                        } catch (Throwable var27) {
                            var2.addSuppressed(var27);
                        }
                    } else {
                        cstmt.close();
                    }
                }
            }
        } catch (SQLException var33) {
            throw new IfxStreamException("Unable to close session", var33);
        }
    }

    private void unwatchTable(IfmxWatchedTable table) throws IfxStreamException, SQLException {
        this.endCapture(table);
        if (this.stopLoggingOnClose) {
            this.setFullRowLogging(table.getDesciptorString(), false);
        }
    }

    private void endCapture(IfmxWatchedTable table) throws IfxStreamException {
        try {
            CallableStatement cstmt = this.con.prepareCall("execute function cdc_endcapture(?,0,?)");
            Throwable var3 = null;

            try {
                cstmt.setInt(1, this.sessionID);
                cstmt.setString(2, table.getDesciptorString());
                ResultSet rs = cstmt.executeQuery();
                Throwable var5 = null;

                try {
                    rs.next();
                    int resultCode = rs.getInt(1);
                    if (resultCode != 0) {
                        throw new IfxStreamException("Unable to end cdc capture. Error code: " + resultCode);
                    }
                } catch (Throwable var30) {
                    var5 = var30;
                    throw var30;
                } finally {
                    if (rs != null) {
                        if (var5 != null) {
                            try {
                                rs.close();
                            } catch (Throwable var29) {
                                var5.addSuppressed(var29);
                            }
                        } else {
                            rs.close();
                        }
                    }
                }
            } catch (Throwable var32) {
                var3 = var32;
                throw var32;
            } finally {
                if (cstmt != null) {
                    if (var3 != null) {
                        try {
                            cstmt.close();
                        } catch (Throwable var28) {
                            var3.addSuppressed(var28);
                        }
                    } else {
                        cstmt.close();
                    }
                }
            }


        } catch (SQLException var34) {
            throw new IfxStreamException("Unable to end cdc capture ", var34);
        }
    }

    public void close() throws Exception {
        if (!this.isClosed) {
            IfxStreamException e = null;
            boolean var15 = false;
            label211:
            {
                IfxStreamException se;
                label212:
                {
                    try {
                        var15 = true;
                        Iterator var2 = this.capturedTables.iterator();

                        while (var2.hasNext()) {
                            IfmxWatchedTable capturedTable = (IfmxWatchedTable) var2.next();
                            this.unwatchTable(capturedTable);
                        }

                        this.closeSession();
                        var15 = false;
                        break label212;
                    } catch (IfxStreamException var22) {
                        e = var22;
                        var15 = false;
                    } finally {
                        if (var15) {
                            this.isClosed = true;
                            try {
                                this.con.close();
                            } catch (SQLException var17) {
                                se = new IfxStreamException("Could not close main connection", var17);
                                if (e == null) {
                                    e = se;
                                } else {
                                    e.addSuppressed(se);
                                }
                            }

                            try {
                                this.recordBuilder.close();
                            } catch (SQLException var16) {
                                se = new IfxStreamException("Could not close record builder", var16);
                                if (e != null) {
                                    e.addSuppressed(se);
                                }
                            }

                        }
                    }

                    this.isClosed = true;

                    try {
                        this.con.close();
                    } catch (SQLException var19) {
                        se = new IfxStreamException("Could not close main connection", var19);
                        if (e == null) {
                            e = se;
                        } else {
                            e.addSuppressed(se);
                        }
                    }

                    try {
                        this.recordBuilder.close();
                    } catch (SQLException var18) {
                        se = new IfxStreamException("Could not close record builder", var18);
                        if (e == null) {
                            e = se;
                        } else {
                            e.addSuppressed(se);
                        }
                    }
                    break label211;
                }

                this.isClosed = true;

                try {
                    this.con.close();
                } catch (SQLException var21) {
                    se = new IfxStreamException("Could not close main connection", var21);
                    if (e == null) {
                        e = se;
                    } else {
                        e.addSuppressed(se);
                    }
                }


                try {
                    this.recordBuilder.close();
                } catch (SQLException var20) {
                    se = new IfxStreamException("Could not close record builder", var20);
                    if (e == null) {
                        e = se;
                    } else {
                        e.addSuppressed(se);
                    }
                }

            }

            if (e != null) {
                throw e;
            }
        }

    }

    public static class IfmxWatchedTable extends IfmxTableDescriptor {
        private static final AtomicInteger counter = new AtomicInteger(1);
        private int label = -1;
        private String[] columns;

        public IfmxWatchedTable(String databaseName, String namespace, String tableName) {
            super(databaseName, namespace, tableName);
            this.label = counter.getAndIncrement();
        }

        public IfmxWatchedTable(IfmxTableDescriptor desc) {
            super(desc.getDatabaseName(), desc.getNamespace(), desc.getTableName());
            this.label = counter.getAndIncrement();
        }

        public String getColumnDescriptorString() {
            return String.join(",", this.columns);
        }

        public String[] getColumns() {
            return this.columns;
        }

        public IfmxWatchedTable columns(String[] columns) {
            this.columns = columns;
            return this;
        }

        public IfmxWatchedTable label(int label) {
            if (label < 0) {
                throw new IllegalArgumentException("Label must be a positive number");
            } else {
                this.label = label;
                return this;
            }
        }

        public int getLabel() {
            return this.label;
        }

        public String toString() {
            return super.toString() + "::" + this.getColumnDescriptorString();
        }
    }


}
