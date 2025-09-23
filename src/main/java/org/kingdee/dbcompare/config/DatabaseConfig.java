package org.kingdee.dbcompare.config;

import lombok.Data;
import java.net.URI;
import java.net.URISyntaxException;

// 数据库连接信息
@Data
public class DatabaseConfig {
    private String url;
    private String username;
    private String password;
    private String name;
    private String schema; // 新增schema字段
    /**
     * -- GETTER --
     *  获取显示名称，用于报告中显示
     */
    private String displayName; // 显示名称，包含IP信息

    public DatabaseConfig(String name, String url, String username, String password, String schema) {
        this.name = name;
        this.url = url;
        this.username = username;
        this.password = password;
        this.schema = schema != null && !schema.trim().isEmpty() ? schema : "public"; // 默认为public
        this.displayName = generateDisplayName(name, url);
    }

    /**
     * 从JDBC URL中提取IP和端口信息，生成显示名称
     * 格式：数据库名(IP:端口)
     */
    private String generateDisplayName(String dbName, String jdbcUrl) {
        try {
            // 解析JDBC URL，例如：jdbc:postgresql://localhost:5432/postgres
            if (jdbcUrl.startsWith("jdbc:postgresql://")) {
                String urlPart = jdbcUrl.substring("jdbc:postgresql://".length());

                // 提取主机和端口部分
                int slashIndex = urlPart.indexOf('/');
                if (slashIndex > 0) {
                    String hostPort = urlPart.substring(0, slashIndex);
                    return String.format("%s(%s)", dbName, hostPort);
                }
            }

            // 如果解析失败，尝试使用URI解析
            URI uri = new URI(jdbcUrl.replace("jdbc:", ""));
            String host = uri.getHost();
            int port = uri.getPort();

            if (host != null) {
                if (port > 0) {
                    return String.format("%s(%s:%d)", dbName, host, port);
                } else {
                    return String.format("%s(%s)", dbName, host);
                }
            }

        } catch (URISyntaxException | StringIndexOutOfBoundsException e) {
            // 解析失败时记录日志但不抛出异常
            System.err.println("无法解析JDBC URL: " + jdbcUrl + ", 错误: " + e.getMessage());
        }

        // 如果无法解析IP信息，返回原数据库名
        return dbName;
    }
}