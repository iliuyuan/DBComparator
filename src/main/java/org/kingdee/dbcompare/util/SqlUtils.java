package org.kingdee.dbcompare.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Pattern;

/**
 * SQL工具类
 */
public class SqlUtils {

    private static final Logger logger = LoggerFactory.getLogger(SqlUtils.class);

    // 表名过滤模式
    private static final Pattern SYSTEM_TABLE_PATTERN = Pattern.compile("^(pg_|information_schema).*");

    /**
     * 测试数据库连接
     */
    public static boolean testConnection(Connection conn) {
        try {
            return conn != null && !conn.isClosed() && conn.isValid(5);
        } catch (SQLException e) {
            logger.warn("数据库连接测试失败", e);
            return false;
        }
    }

    /**
     * 获取数据库版本信息
     */
    public static String getDatabaseVersion(Connection conn) throws SQLException {
        try (var stmt = conn.createStatement();
             var rs = stmt.executeQuery("SELECT version()")) {

            if (rs.next()) {
                return rs.getString(1);
            }
            return "Unknown";
        }
    }

    /**
     * 检查是否为系统表
     */
    public static boolean isSystemTable(String tableName) {
        return SYSTEM_TABLE_PATTERN.matcher(tableName).matches();
    }

    /**
     * 检查表名是否匹配模式
     */
    public static boolean matchesPattern(String tableName, String pattern) {
        if (pattern == null || pattern.trim().isEmpty()) {
            return true;
        }

        try {
            Pattern compiled = Pattern.compile(pattern);
            return compiled.matcher(tableName).matches();
        } catch (Exception e) {
            logger.warn("无效的正则表达式模式: {}", pattern);
            return false;
        }
    }

    /**
     * 构建表查询SQL
     */
    public static String buildTableQuerySql(String schemaName) {
        return String.format(
                "SELECT table_name, table_type " +
                        "FROM information_schema.tables " +
                        "WHERE table_schema = '%s' " +
                        "AND table_type = 'BASE TABLE' " +
                        "ORDER BY table_name",
                schemaName
        );
    }

    /**
     * 构建列查询SQL
     */
    public static String buildColumnQuerySql(String schemaName) {
        return String.format(
                "SELECT table_name, column_name, data_type, is_nullable, " +
                        "column_default, character_maximum_length, numeric_precision, numeric_scale, " +
                        "ordinal_position " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = '%s' " +
                        "ORDER BY table_name, ordinal_position",
                schemaName
        );
    }

    /**
     * 构建主键查询SQL
     */
    public static String buildPrimaryKeyQuerySql(String schemaName) {
        return String.format(
                "SELECT tc.table_name, kcu.column_name, kcu.ordinal_position " +
                        "FROM information_schema.table_constraints tc " +
                        "JOIN information_schema.key_column_usage kcu " +
                        "ON tc.constraint_name = kcu.constraint_name " +
                        "WHERE tc.constraint_type = 'PRIMARY KEY' " +
                        "AND tc.table_schema = '%s' " +
                        "ORDER BY tc.table_name, kcu.ordinal_position",
                schemaName
        );
    }

    /**
     * 构建索引查询SQL
     */
    public static String buildIndexQuerySql(String schemaName) {
        return String.format(
                "SELECT " +
                        "    i.schemaname, " +
                        "    i.tablename, " +
                        "    i.indexname, " +
                        "    i.indexdef, " +
                        "    ix.indisunique, " +
                        "    ix.indisprimary " +
                        "FROM pg_indexes i " +
                        "JOIN pg_class c ON c.relname = i.indexname " +
                        "JOIN pg_index ix ON ix.indexrelid = c.oid " +
                        "WHERE i.schemaname = '%s' " +
                        "AND NOT ix.indisprimary " +  // 排除主键索引
                        "ORDER BY i.tablename, i.indexname",
                schemaName
        );
    }

    /**
     * 构建外键查询SQL
     */
    public static String buildForeignKeyQuerySql(String schemaName) {
        return String.format(
                "SELECT " +
                        "    tc.table_name, " +
                        "    kcu.column_name, " +
                        "    ccu.table_name AS foreign_table_name, " +
                        "    ccu.column_name AS foreign_column_name, " +
                        "    tc.constraint_name " +
                        "FROM information_schema.table_constraints AS tc " +
                        "JOIN information_schema.key_column_usage AS kcu " +
                        "    ON tc.constraint_name = kcu.constraint_name " +
                        "JOIN information_schema.constraint_column_usage AS ccu " +
                        "    ON ccu.constraint_name = tc.constraint_name " +
                        "WHERE tc.constraint_type = 'FOREIGN KEY' " +
                        "AND tc.table_schema = '%s' " +
                        "ORDER BY tc.table_name, kcu.ordinal_position",
                schemaName
        );
    }

    /**
     * 清理资源
     */
    public static void closeQuietly(AutoCloseable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception e) {
                logger.debug("关闭资源时发生异常", e);
            }
        }
    }
}