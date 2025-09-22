package org.kingdee.dbcompare;


import org.kingdee.dbcompare.config.DatabaseConfig;
import org.kingdee.dbcompare.service.PgSQLDBComparator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@SpringBootApplication
public class SchemaComparatorApp {
    public static void main(String[] args) {
        SpringApplication.run(SchemaComparatorApp.class, args);
    }

    // 使用示例
    public static void comparator(String[] args) {
        try {
            // 配置数据库连接
            DatabaseConfig baseDb = new DatabaseConfig(
                    "基准数据库A",
                    "jdbc:postgresql://localhost:5432/database_a",
                    "username", "password"
            );

            List<DatabaseConfig> targetDbs = Arrays.asList(
                    new DatabaseConfig("数据库B",
                            "jdbc:postgresql://localhost:5432/database_b", "username", "password"),
                    new DatabaseConfig("数据库C",
                            "jdbc:postgresql://localhost:5432/database_c", "username", "password"),
                    new DatabaseConfig("数据库D",
                            "jdbc:postgresql://localhost:5432/database_d", "username", "password")
            );

            // 创建对比器并执行对比
            PgSQLDBComparator comparator = new PgSQLDBComparator(baseDb, targetDbs);
            comparator.compareSchemas();

            // 打印报告
            comparator.printDifferencesReport();

            // 导出到CSV
            comparator.exportDifferencesToCSV("schema_differences.csv");

        } catch (SQLException | java.io.IOException e) {
            e.printStackTrace();
        }
    }
}