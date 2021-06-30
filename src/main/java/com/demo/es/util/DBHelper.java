package com.demo.es.util;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;

@Component
public class DBHelper {
    @Value("${database.mysql.url}")
    public String url;
    @Value("${database.mysql.dname}")
    public String name;
    @Value("${database.mysql.username}")
    public String user;
    @Value("${database.mysql.password}")
    public String password;
    private static Connection connection = null;

    public Connection getConn() {
        try {
            Class.forName(name);
            connection = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }
}
