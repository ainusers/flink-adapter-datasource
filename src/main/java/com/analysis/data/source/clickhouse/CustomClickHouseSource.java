package com.analysis.data.source.clickhouse;

import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: tianyong
 * @time: 2021/10/20 15:12
 * @description:
 * @Version: v1.0
 * @company: xxx Group.Situation xxx事业部
 */
public class CustomClickHouseSource {
    private static final String URL = "jdbc:clickhouse://192.168.43.43:8123/default";
    private static final String USER = "default";
    private static final String PASSWORD = "root1234";

    public static void main(String[] args) {
        Connection connection = null;
        // 注意这里使用的CK自己实现的PreparedStatement
        ClickHousePreparedStatement statement = null;

        try {
            // 注册JDBC驱动
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");

            // 打开连接
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
            System.out.println("connected database successfully");

            // 执行查询
            String sql = "select id,name,age from default.test where id = ?";
            statement = (ClickHousePreparedStatement) connection.prepareStatement(sql);
            statement.setString(1, "1");
            ResultSet rs = statement.executeQuery();

            // 打印填充后的SQL语句（ck实现的PreparedStatement类包含了打印sql语句的方法）
            System.out.println("execute sql: " + statement.asSql());

            // 从结果集中提取数据
            List<Map> list = new ArrayList();
            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next()) {
                Map map = new HashMap();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    map.put(rsmd.getColumnName(i), rs.getString(rsmd.getColumnName(i)));
                }
                list.add(map);
            }
            list.forEach(System.out::println);
            rs.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            // 释放资源
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }
}
