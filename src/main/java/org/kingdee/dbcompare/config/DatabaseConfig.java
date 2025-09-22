package org.kingdee.dbcompare.config;

import lombok.Data;

// 数据库连接信息
@Data
public class DatabaseConfig {
    private String url;
    private String username;
    private String password;
    private String name;
    private String schema; // 新增schema字段

    public DatabaseConfig(String name, String url, String username, String password, String schema) {
        this.name = name;
        this.url = url;
        this.username = username;
        this.password = password;
        this.schema = schema != null && !schema.trim().isEmpty() ? schema : "public"; // 默认为public
    }

    // 保持向后兼容的构造函数
    public DatabaseConfig(String name, String url, String username, String password) {
        this(name, url, username, password, "public");
    }
}