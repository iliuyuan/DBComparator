//package org.kingdee.dbcompare;
//
//import org.kingdee.dbcompare.config.ConfigManager;
//import org.kingdee.dbcompare.service.PgSQLDBComparator;
//import org.kingdee.dbcompare.service.ReportGenerator;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.CommandLineRunner;
//import org.springframework.boot.SpringApplication;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//
//import java.time.LocalDateTime;
//import java.time.format.DateTimeFormatter;
//
//@SpringBootApplication
//public class SchemaComparatorApp implements CommandLineRunner {
//
//    private static final Logger logger = LoggerFactory.getLogger(SchemaComparatorApp.class);
//
//    @Autowired
//    private ReportGenerator reportGenerator;
//
//    public static void main(String[] args) {
//        SpringApplication.run(SchemaComparatorApp.class, args);
//    }
//
//    @Override
//    public void run(String... args) throws Exception {
//        logger.info("=== PostgreSQL 数据库结构对比工具启动 ===");
//
//        try {
//            // 使用默认配置执行对比
//            executeDefaultComparison();
//
//        } catch (Exception e) {
//            logger.error("执行过程中发生错误", e);
//            throw e;
//        }
//    }
//
//    /**
//     * 执行默认配置的对比
//     */
//    private void executeDefaultComparison() throws Exception {
//        String configFile = "src/main/resources/database-config.properties";
//
//        // 加载数据库配置
//        ConfigManager.DatabaseConfigSet configSet = ConfigManager.loadDatabaseConfig(configFile);
//
//        logger.info("基准数据库: {}", configSet.getBaseDatabase().getName());
//        logger.info("目标数据库数量: {}", configSet.getTargetDatabases().size());
//
//        // 执行对比
//        PgSQLDBComparator comparator = new PgSQLDBComparator(
//                configSet.getBaseDatabase(),
//                configSet.getTargetDatabases()
//        );
//
//        comparator.compareSchemas();
//
//        // 打印控制台报告
//        comparator.printDifferencesReport();
//
//        // 生成多种格式的报告
//        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
//        String baseDir = "C:\\Users\\HP\\Desktop\\dbexport\\";
//
//        // 确保输出目录存在
//        java.nio.file.Files.createDirectories(java.nio.file.Paths.get(baseDir));
//
//        // 导出各种格式
//        String csvFile = baseDir + "schema_differences_" + timestamp + ".csv";
//        String jsonFile = baseDir + "schema_differences_" + timestamp + ".json";
//        String htmlFile = baseDir + "schema_differences_" + timestamp + ".html";
//
//        comparator.exportDifferencesToCSV(csvFile);
//        reportGenerator.exportToJSON(comparator.getDifferences(), jsonFile);
//        reportGenerator.exportToHTML(comparator.getDifferences(), htmlFile);
//
//        logger.info("报告已生成:");
//        logger.info("  CSV: {}", csvFile);
//        logger.info("  JSON: {}", jsonFile);
//        logger.info("  HTML: {}", htmlFile);
//    }
//}