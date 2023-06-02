package yashang;

//Jdbcexample.java
//演示基于JDBC开发的主要步骤，涉及创建数据库、创建表、插入数据等。

import java.sql.*;

public class Jdbcexample {
    //创建数据库连接。
    public static Connection getConnection(String username, String passwd) {
        String driver = "com.yashandb.jdbc.Driver";
        String sourceURL = "jdbc:yasdb://110.00.00.000:0000/yasdb";
        Connection conn = null;
        try {
            //加载数据库驱动。
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        try {
            //创建数据库连接。
            conn = DriverManager.getConnection(sourceURL, username, passwd);
            System.out.println("Connection succeed!");
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        return conn;
    }

    //执行普通SQL语句，创建customer表。
    public static void createTable(Connection conn) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();

            //执行普通SQL语句。
            stmt.execute("CREATE TABLE customer2(id INTEGER, name VARCHAR(32))");
            System.out.println("create table customer succeed!");
            stmt.close();
        } catch (SQLException e) {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            e.printStackTrace();
        }
    }

    //执行预处理语句，批量插入数据。
    public static void batchInsertData(Connection conn) {
        PreparedStatement pst = null;

        try {
            //生成预处理语句。
            String sql = "INSERT INTO " + "customer VALUES (?,?)";
            pst = conn.prepareStatement("INSERT INTO customer VALUES (?,?)");
            for (int i = 0; i < 3; i++) {
                //添加参数。
                pst.setInt(1, i);
                pst.setString(2, "sales" + i);
                pst.addBatch();
            }
            //执行批处理。
            pst.executeBatch();
            System.out.println("insert table customer succeed!");
            pst.close();
        } catch (SQLException e) {
            if (pst != null) {
                try {
                    pst.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            e.printStackTrace();
        }
    }

    //修改语句
    public static void update(Connection conn) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();

            //执行普通SQL语句。
            stmt.execute("UPDATE CUSTOMER SET NAME = 666 WHERE ID=2");
            System.out.println("UPDATE table customer succeed!");
            stmt.close();
        } catch (SQLException e) {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            e.printStackTrace();
        }

    }


    //删除语句
    public static void delete(Connection conn) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();

            //执行普通SQL语句。
            stmt.execute("DELETE FROM CUSTOMER WHERE ID=2");
            System.out.println("delete table customer succeed!");
            stmt.close();
        } catch (SQLException e) {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            e.printStackTrace();
        }

    }

    //删除表
    public static void dropTable(Connection conn) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();

            //执行普通SQL语句。
            stmt.execute("DROP TABLE IF EXISTS CUSTOMER");
            System.out.println("drop table customer succeed!");
            stmt.close();
        } catch (SQLException e) {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            e.printStackTrace();
        }

    }

    //清空表
    public static void clearTable(Connection conn) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();

            //执行普通SQL语句。
            stmt.execute("TRUNCATE TABLE CUSTOMER");
            System.out.println("clear table customer succeed!");
            stmt.close();
        } catch (SQLException e) {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            e.printStackTrace();
        }

    }


    //查询数据库所有的表名
    public static void allTable(Connection conn) {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();

            //执行普通SQL语句。
            rs = stmt.executeQuery("SELECT \"date_field\" FROM \"TAPDATA\".\"sk_amy\" WHERE \"_id\"  = '643e8833d8d65654a5cc0ba2'");
            System.out.println("select table customer succeed!");

            // 处理查询结果
            while (rs.next()) {
                String tableName = rs.getString("date_field");
                System.out.println("Table Name: " + tableName);
            }

            stmt.close();
        } catch (SQLException e) {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            e.printStackTrace();
        }

    }

    //查询数据库是否存在表
    public static void existTable(Connection conn) {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();

            //执行普通SQL语句。
            rs = stmt.executeQuery("SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = 'TAPDATA' AND TABLE_NAME = 'SK'");
            System.out.println("table exist");
            while (rs.next()) {
                String tableName = rs.getString("COUNT(*)");
                System.out.println(tableName );
            }
            stmt.close();
        } catch (SQLException e) {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            e.printStackTrace();
        }

    }

    public static void allColumns(Connection conn) {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();

            //执行普通SQL语句。
            rs = stmt.executeQuery("SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE FROM ALL_TAB_COLUMNS WHERE OWNER = 'TAPDATA' AND TABLE_NAME = 'CUSTOMER'");
            System.out.println("query allColumns succeed!");

            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String dataType = rs.getString("DATA_TYPE");
                int dataLength = rs.getInt("DATA_LENGTH");
                int dataPrecision = rs.getInt("DATA_PRECISION");
                int dataScale = rs.getInt("DATA_SCALE");
                String isNullable = rs.getString("NULLABLE");

                System.out.println("Column Name: " + columnName);
                System.out.println("Data Type: " + dataType);
                System.out.println("Data Length: " + dataLength);
                System.out.println("Data Precision: " + dataPrecision);
                System.out.println("Data Scale: " + dataScale);
                System.out.println("Is Nullable: " + isNullable);
                System.out.println("-----------------------------");
            }
            stmt.close();
        } catch (SQLException e) {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            e.printStackTrace();
        }
    }

    //查询当前表的模型

    public static int execJdbcexample(String ctrls) {
        //创建数据库连接。
        //输入用户名 密码
        Connection conn = getConnection("******", "******");

        //创建表。
//        createTable(conn);

//        批插数据。
//        batchInsertData(conn);

//        update(conn);

//        delete(conn);

//        dropTable(conn);

//        clearTable(conn);

//        allTable(conn);

//        allColumns(conn);

//        existTable(conn);

        //关闭数据库连接。
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return 0;
        }
        return 1;
    }

    /**
     * 主程序，逐步调用各静态方法。
     *
     * @param args
     */
    public static void main(String[] args) {
        int a = execJdbcexample("1");
    }
}
