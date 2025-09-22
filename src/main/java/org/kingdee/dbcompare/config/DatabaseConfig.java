package org.kingdee.dbcompare.config;

import lombok.Data;

// 数据库连接信息
@Data
public class DatabaseConfig {
    private String url;
    private String username;
    private String password;
    private String name;

    public DatabaseConfig(String name, String url, String username, String password) {
        this.name = name;
        this.url = url;
        this.username = username;
        this.password = password;
    }
}