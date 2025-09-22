package org.kingdee.dbcompare;

import org.kingdee.dbcompare.config.ConfigManager;
import org.kingdee.dbcompare.service.EnhancedReportGenerator;
import org.kingdee.dbcompare.service.PgSQLDBComparator;
import org.kingdee.dbcompare.service.ReportGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@SpringBootApplication
public class SmartSchemaComparatorApp implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(SmartSchemaComparatorApp.class);

    @Autowired
    private ReportGenerator reportGenerator;

    @Autowired
    private EnhancedReportGenerator enhancedReportGenerator;

    // 配置阈值：当目标数据库数量超过此值时，启用多层级报告模式
    private static final int MULTI_LEVEL_THRESHOLD = 5;

    public static void main(String[] args) {
        SpringApplication.run(SmartSchemaComparatorApp.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("=== PostgreSQL 智能数据库结构对比工具启动 ===");

        try {
            executeSmartComparison();
        } catch (Exception e) {
            logger.error("执行过程中发生错误", e);
            throw e;
        }
    }

    /**
     * 执行智能化对比（根据数据库数量自动选择报告策略）
     */
    private void executeSmartComparison() throws Exception {
        String configFile = "src/main/resources/database-config.properties";

        // 加载数据库配置
        ConfigManager.DatabaseConfigSet configSet = ConfigManager.loadDatabaseConfig(configFile);
        int targetCount = configSet.getTargetDatabases().size();

        logger.info("基准数据库: {}", configSet.getBaseDatabase().getDisplayName());
        logger.info("目标数据库数量: {}", targetCount);

        // 执行对比
        PgSQLDBComparator comparator = new PgSQLDBComparator(
                configSet.getBaseDatabase(),
                configSet.getTargetDatabases()
        );

        long startTime = System.currentTimeMillis();
        comparator.compareSchemas();
        long compareTime = System.currentTimeMillis() - startTime;

        logger.info("数据库结构对比完成，耗时: {}ms", compareTime);

        // 打印控制台报告
        comparator.printDifferencesReport();

        // 根据目标数据库数量选择报告生成策略
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String baseDir = "C:\\Users\\HP\\Desktop\\dbexport";

        if (targetCount <= MULTI_LEVEL_THRESHOLD) {
            // 少量数据库：使用传统单文件报告
            generateTraditionalReports(comparator, baseDir, timestamp);
        } else {
            // 大量数据库：使用多层级报告
            generateMultiLevelReports(comparator, baseDir, timestamp);
        }
    }

    /**
     * 生成传统单文件报告（适用于少量数据库）
     */
    private void generateTraditionalReports(PgSQLDBComparator comparator,
                                            String baseDir,
                                            String timestamp) throws Exception {

        logger.info("生成传统单文件报告...");

        // 确保输出目录存在
        java.nio.file.Files.createDirectories(java.nio.file.Paths.get(baseDir));

        // 导出各种格式到单个文件
        String csvFile = baseDir + "/schema_differences_" + timestamp + ".csv";
        String jsonFile = baseDir + "/schema_differences_" + timestamp + ".json";
        String htmlFile = baseDir + "/schema_differences_" + timestamp + ".html";

        comparator.exportDifferencesToCSV(csvFile);
        reportGenerator.exportToJSON(comparator.getDifferences(), jsonFile);
        reportGenerator.exportToHTML(comparator.getDifferences(), htmlFile);

        logger.info("传统报告已生成:");
        logger.info("  CSV: {}", csvFile);
        logger.info("  JSON: {}", jsonFile);
        logger.info("  HTML: {}", htmlFile);
    }

    /**
     * 生成多层级报告（适用于大量数据库）
     */
    private void generateMultiLevelReports(PgSQLDBComparator comparator,
                                           String baseDir,
                                           String timestamp) throws Exception {

        logger.info("检测到 {} 个目标数据库，启用多层级报告模式...", comparator.getDifferences().size());

        long startTime = System.currentTimeMillis();

        // 使用增强报告生成器
        EnhancedReportGenerator.ReportResult result = enhancedReportGenerator.generateMultiLevelReports(
                comparator.getDifferences(),
                baseDir,
                timestamp
        );

        long reportTime = System.currentTimeMillis() - startTime;

        logger.info("多层级报告生成完成:");
        logger.info("  报告生成耗时: {}ms", reportTime);
        logger.info("  总计生成文件: {} 个", result.getTotalFiles());
        logger.info("  主索引文件: {}", result.getIndexFile());

        // 显示主要报告文件
        printReportSummary(result);

        // 生成报告使用指南
        generateReportGuide(result, baseDir, timestamp);
    }

    /**
     * 打印报告摘要
     */
    private void printReportSummary(EnhancedReportGenerator.ReportResult result) {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("               📊 报告生成摘要");
        System.out.println("=".repeat(60));

        System.out.println("📋 主要报告文件:");
        result.getFiles().entrySet().stream()
                .filter(entry -> entry.getKey().contains("总览") || entry.getKey().contains("索引"))
                .forEach(entry -> System.out.println("  " + entry.getKey() + ": " + entry.getValue()));

        System.out.println("\n🔍 使用建议:");
        System.out.println("  1. 首先查看 index.html 获取全局概览");
        System.out.println("  2. 查看 summary_overview.html 了解统计信息");
        System.out.println("  3. 针对问题数据库查看 by_database/ 目录下的详细报告");
        System.out.println("  4. 按问题类型批量处理可查看 by_type/ 目录");

        System.out.println("=".repeat(60));
    }

    /**
     * 生成报告使用指南
     */
    private void generateReportGuide(EnhancedReportGenerator.ReportResult result,
                                     String baseDir,
                                     String timestamp) throws Exception {

        String guideFile = baseDir + "/schema_compare_" + timestamp + "/README.md";

        StringBuilder guide = new StringBuilder();
        guide.append("# 数据库结构差异报告使用指南\n\n");
        guide.append("生成时间: ").append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))).append("\n\n");

        guide.append("## 📁 目录结构\n\n");
        guide.append("```\n");
        guide.append("schema_compare_").append(timestamp).append("/\n");
        guide.append("├── index.html              # 主索引页面（推荐首先查看）\n");
        guide.append("├── README.md               # 本使用指南\n");
        guide.append("├── summary/                # 总览报告\n");
        guide.append("│   ├── summary_overview.csv\n");
        guide.append("│   ├── summary_overview.json\n");
        guide.append("│   └── summary_overview.html\n");
        guide.append("├── by_database/            # 按数据库分组的详细报告\n");
        guide.append("│   ├── 数据库A/\n");
        guide.append("│   │   ├── 数据库A_details.csv\n");
        guide.append("│   │   ├── 数据库A_details.json\n");
        guide.append("│   │   └── 数据库A_details.html\n");
        guide.append("│   └── 数据库B/\n");
        guide.append("│       └── ...\n");
        guide.append("└── by_type/                # 按差异类型分组的报告\n");
        guide.append("    ├── 缺少表.csv\n");
        guide.append("    ├── 缺少列.csv\n");
        guide.append("    └── ...\n");
        guide.append("```\n\n");

        guide.append("## 🚀 快速开始\n\n");
        guide.append("1. **总体概览**: 打开 `index.html` 查看整体情况\n");
        guide.append("2. **问题定位**: 在总览中找到问题最多的数据库\n");
        guide.append("3. **详细分析**: 进入对应数据库目录查看详细报告\n");
        guide.append("4. **批量处理**: 使用 `by_type/` 目录按类型处理问题\n\n");

        guide.append("## 📊 报告类型说明\n\n");
        guide.append("### 总览报告 (summary/)\n");
        guide.append("- **HTML**: 可视化展示，包含图表和统计信息\n");
        guide.append("- **CSV**: 适合Excel分析，包含各数据库统计\n");
        guide.append("- **JSON**: 程序化处理，包含完整统计数据\n\n");

        guide.append("### 数据库详细报告 (by_database/)\n");
        guide.append("- 每个目标数据库都有独立的详细报告\n");
        guide.append("- 包含该数据库与基准数据库的所有差异\n");
        guide.append("- 支持CSV、JSON、HTML三种格式\n\n");

        guide.append("### 类型分组报告 (by_type/)\n");
        guide.append("- 按差异类型（缺少表、多余列等）分组\n");
        guide.append("- 便于批量处理同类型问题\n");
        guide.append("- 支持CSV、JSON格式\n\n");

        guide.append("## ⚠️ 差异严重性说明\n\n");
        guide.append("- **🔴 严重**: 缺少表、主键差异 - 需要优先处理\n");
        guide.append("- **🟡 警告**: 缺少列、列定义差异、缺少索引 - 需要关注\n");
        guide.append("- **🔵 一般**: 多余表、多余列、多余索引 - 可选处理\n\n");

        guide.append("## 💡 使用建议\n\n");
        guide.append("1. **优先处理严重问题**: 关注缺少表和主键差异\n");
        guide.append("2. **分批处理**: 不要一次处理所有数据库，建议分批进行\n");
        guide.append("3. **备份为先**: 修改数据库结构前请务必备份\n");
        guide.append("4. **测试验证**: 在测试环境先验证修改脚本\n");
        guide.append("5. **记录变更**: 建议记录每次修改的详细日志\n\n");

        guide.append("---\n");
        guide.append("*报告由 PostgreSQL Schema Comparator 自动生成*\n");

        // 写入指南文件
        try (java.io.FileWriter writer = new java.io.FileWriter(guideFile)) {
            writer.write(guide.toString());
        }

        logger.info("使用指南已生成: {}", guideFile);
    }
}