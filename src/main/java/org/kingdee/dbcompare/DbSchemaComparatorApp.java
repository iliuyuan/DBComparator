package org.kingdee.dbcompare;

import lombok.Getter;
import org.kingdee.dbcompare.config.DatabaseConfigManager;
import org.kingdee.dbcompare.config.DatabaseComparatorConfig;
import org.kingdee.dbcompare.model.SchemaDifference;
import org.kingdee.dbcompare.service.PgSQLDBComparator;
import org.kingdee.dbcompare.service.ReportGeneratorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 优化版多数据库分文件对比工具
 * 特点：
 * 1. 固定文件命名格式，自动覆盖旧报告
 * 2. 文件名明确体现对比关系：基准库_vs_目标库
 * 3. 支持增量更新，只对比发生变化的数据库
 */
@SpringBootApplication
public class DbSchemaComparatorApp implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(DbSchemaComparatorApp.class);

    @Autowired
    private ReportGeneratorService reportGeneratorService;

    // 并发处理线程数
    private static final int THREAD_POOL_SIZE = 8;
    // 批处理大小
    private static final int BATCH_SIZE = 10;
    // 输出基目录
    private static final String BASE_OUTPUT_DIR = "C:\\Users\\HP\\Desktop\\dbexport\\schema_comparisons";

    public static void main(String[] args) {
        SpringApplication.run(DbSchemaComparatorApp.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("=== PostgreSQL 优化版多数据库对比工具启动 ===");

        try {
            executeOptimizedComparison();
        } catch (Exception e) {
            logger.error("执行过程中发生错误", e);
            throw e;
        }
    }

    /**
     * 执行优化的多数据库对比
     */
    private void executeOptimizedComparison() throws Exception {
        String configFile = "src/main/resources/database-config.properties";

        // 加载数据库配置
        DatabaseConfigManager.DatabaseConfigSet configSet = DatabaseConfigManager.loadDatabaseConfig(configFile);
        DatabaseComparatorConfig baseDatabase = configSet.getBaseDatabase();
        List<DatabaseComparatorConfig> targetDatabases = configSet.getTargetDatabases();

        logger.info("基准数据库: {}", baseDatabase.getDisplayName());
        logger.info("目标数据库数量: {}", targetDatabases.size());

        // 创建输出目录结构
        setupOutputDirectories(baseDatabase);

        // 记录本次对比的开始时间
        LocalDateTime comparisonTime = LocalDateTime.now();

        // 使用线程池并发处理
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        try {
            // 分批处理数据库
            ComparisonResult result = processDatabasesInBatches(baseDatabase, targetDatabases, executor);

            // 生成或更新总览报告
            generateMasterSummary(baseDatabase, targetDatabases, result, comparisonTime);

            // 生成对比历史记录
            updateComparisonHistory(baseDatabase, result, comparisonTime);

        } finally {
            shutdownExecutor(executor);
        }

        logger.info("=== 所有数据库对比完成 ===");
        logger.info("报告目录: {}", BASE_OUTPUT_DIR);
    }

    /**
     * 设置输出目录结构
     */
    private void setupOutputDirectories(DatabaseComparatorConfig baseDatabase) throws Exception {
        String safeBaseName = sanitizeFileName(baseDatabase.getName());
        String baseDbDir = BASE_OUTPUT_DIR + "/" + safeBaseName;

        // 创建基准数据库的报告目录
        Files.createDirectories(Paths.get(baseDbDir));
        Files.createDirectories(Paths.get(baseDbDir + "/comparisons"));
        Files.createDirectories(Paths.get(baseDbDir + "/summaries"));
        Files.createDirectories(Paths.get(baseDbDir + "/history"));

        logger.info("报告目录结构已创建: {}", baseDbDir);
    }

    /**
     * 分批处理数据库
     */
    private ComparisonResult processDatabasesInBatches(DatabaseComparatorConfig baseDatabase,
                                                       List<DatabaseComparatorConfig> targetDatabases,
                                                       ExecutorService executor) {

        ComparisonResult result = new ComparisonResult();
        int totalDatabases = targetDatabases.size();
        int processedCount = 0;

        // 分批处理
        for (int i = 0; i < totalDatabases; i += BATCH_SIZE) {
            int endIndex = Math.min(i + BATCH_SIZE, totalDatabases);
            List<DatabaseComparatorConfig> batch = targetDatabases.subList(i, endIndex);

            logger.info("处理第 {} 批数据库，包含数据库 {} - {} / {}",
                    (i / BATCH_SIZE + 1), i + 1, endIndex, totalDatabases);

            // 并发处理当前批次
            List<CompletableFuture<DatabaseComparisonResult>> futures = new ArrayList<>();

            for (DatabaseComparatorConfig targetDb : batch) {
                CompletableFuture<DatabaseComparisonResult> future = CompletableFuture.supplyAsync(() -> {
                    try {
                        return processSingleDatabase(baseDatabase, targetDb);
                    } catch (Exception e) {
                        logger.error("处理数据库 {} 时发生错误", targetDb.getDisplayName(), e);
                        return new DatabaseComparisonResult(targetDb, false, 0);
                    }
                }, executor);

                futures.add(future);
            }

            // 等待当前批次完成并收集结果
            for (CompletableFuture<DatabaseComparisonResult> future : futures) {
                DatabaseComparisonResult dbResult = future.join();
                result.addDatabaseResult(dbResult);
            }

            processedCount += batch.size();
            logger.info("已完成 {} / {} 个数据库的对比", processedCount, totalDatabases);
        }

        return result;
    }

    /**
     * 处理单个数据库
     */
    private DatabaseComparisonResult processSingleDatabase(DatabaseComparatorConfig baseDatabase,
                                                           DatabaseComparatorConfig targetDatabase) {

        logger.info("开始处理数据库: {}", targetDatabase.getDisplayName());
        long startTime = System.currentTimeMillis();

        try {
            // 创建单数据库对比器
            List<DatabaseComparatorConfig> singleTargetList = List.of(targetDatabase);
            PgSQLDBComparator comparator = new PgSQLDBComparator(baseDatabase, singleTargetList);

            // 执行对比
            comparator.compareSchemas();
            List<SchemaDifference> differences = comparator.getDifferences();

            // 生成固定命名的报告文件
            generateStandardReports(baseDatabase, targetDatabase, differences);

            long duration = System.currentTimeMillis() - startTime;
            logger.info("数据库 {} 处理完成，用时 {}ms，发现 {} 个差异",
                    targetDatabase.getDisplayName(), duration, differences.size());

            return new DatabaseComparisonResult(targetDatabase, true, differences.size());

        } catch (Exception e) {
            logger.error("处理数据库 {} 失败", targetDatabase.getDisplayName(), e);
            return new DatabaseComparisonResult(targetDatabase, false, 0);
        }
    }

    /**
     * 生成标准命名的报告文件
     */
    private void generateStandardReports(DatabaseComparatorConfig baseDatabase,
                                         DatabaseComparatorConfig targetDatabase,
                                         List<SchemaDifference> differences) throws Exception {

        String safeBaseName = sanitizeFileName(baseDatabase.getName());
        String safeTargetName = sanitizeFileName(targetDatabase.getName());

        // 构建标准文件名：基准库_vs_目标库
        String filePrefix = safeBaseName + "_vs_" + safeTargetName;

        // 报告文件保存在基准库目录下的comparisons子目录
        String comparisonDir = BASE_OUTPUT_DIR + "/" + safeBaseName + "/comparisons";
        Files.createDirectories(Paths.get(comparisonDir));

        // 生成各种格式的报告（固定文件名，自动覆盖）
        String csvFile = comparisonDir + "/" + filePrefix + "_differences.csv";
        String jsonFile = comparisonDir + "/" + filePrefix + "_differences.json";
        String htmlFile = comparisonDir + "/" + filePrefix + "_differences.html";
        String summaryFile = comparisonDir + "/" + filePrefix + "_summary.txt";

        // 导出报告
        exportDifferencesToCSV(differences, csvFile);
        reportGeneratorService.exportToJSON(differences, jsonFile);
        reportGeneratorService.exportToHTML(differences, htmlFile);

        // 生成详细摘要
        generateDetailedSummary(baseDatabase, targetDatabase, differences, summaryFile);

        logger.debug("数据库 {} 的报告已生成到: {}", targetDatabase.getDisplayName(), comparisonDir);
    }

    /**
     * 生成详细摘要文件
     */
    private void generateDetailedSummary(DatabaseComparatorConfig baseDatabase,
                                         DatabaseComparatorConfig targetDatabase,
                                         List<SchemaDifference> differences,
                                         String summaryFile) throws Exception {

        StringBuilder summary = new StringBuilder();

        // 标题信息
        summary.append("数据库结构对比详细报告\n");
        summary.append("=".repeat(60)).append("\n");
        summary.append("基准数据库: ").append(baseDatabase.getDisplayName()).append("\n");
        summary.append("目标数据库: ").append(targetDatabase.getDisplayName()).append("\n");
        summary.append("基准Schema: ").append(baseDatabase.getSchema()).append("\n");
        summary.append("目标Schema: ").append(targetDatabase.getSchema()).append("\n");
        summary.append("对比时间: ").append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))).append("\n");
        summary.append("总差异数: ").append(differences.size()).append("\n\n");

        if (differences.isEmpty()) {
            summary.append("✅ 完全一致！\n");
            summary.append("该目标数据库与基准数据库结构完全一致，未发现任何差异。\n");
        } else {
            // 差异统计
            Map<String, Long> typeStats = differences.stream()
                    .collect(java.util.stream.Collectors.groupingBy(
                            diff -> diff.getType().getDescription(),
                            java.util.LinkedHashMap::new,
                            java.util.stream.Collectors.counting()
                    ));

            summary.append("📊 差异类型统计:\n");
            summary.append("-".repeat(40)).append("\n");
            typeStats.forEach((type, count) ->
                    summary.append(String.format("  %-20s: %3d 个\n", type, count)));

            // 严重程度分析
            long criticalCount = differences.stream()
                    .mapToLong(diff -> isCritical(diff.getType()) ? 1 : 0)
                    .sum();

            long warningCount = differences.stream()
                    .mapToLong(diff -> isWarning(diff.getType()) ? 1 : 0)
                    .sum();

            long infoCount = differences.size() - criticalCount - warningCount;

            summary.append(String.format("\n🎯 严重程度分析:\n"));
            summary.append("-".repeat(40)).append("\n");
            summary.append(String.format("  🔴 严重问题: %3d 个  (缺少表、主键差异)\n", criticalCount));
            summary.append(String.format("  🟡 警告问题: %3d 个  (缺少列、列差异、缺少索引)\n", warningCount));
            summary.append(String.format("  🔵 一般问题: %3d 个  (多余表、多余列、多余索引)\n", infoCount));

            // 处理建议
            summary.append("\n💡 处理建议:\n");
            summary.append("-".repeat(40)).append("\n");
            if (criticalCount > 0) {
                summary.append("  1. 🚨 优先处理严重问题，特别是缺少的表和主键差异\n");
            }
            if (warningCount > 0) {
                summary.append("  2. ⚠️  关注警告问题，可能影响应用功能\n");
            }
            if (infoCount > 0) {
                summary.append("  3. ℹ️  一般问题可选择性处理，通常不影响核心功能\n");
            }

            // 按表分组统计（显示问题最多的表）
            Map<String, Long> tableStats = differences.stream()
                    .collect(java.util.stream.Collectors.groupingBy(
                            SchemaDifference::getTableName,
                            java.util.stream.Collectors.counting()
                    ));

            if (!tableStats.isEmpty()) {
                summary.append("\n📋 问题表排行榜 (前10):\n");
                summary.append("-".repeat(40)).append("\n");

                tableStats.entrySet().stream()
                        .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                        .limit(10)
                        .forEach(entry ->
                                summary.append(String.format("  %-25s: %3d 个差异\n",
                                        entry.getKey(), entry.getValue())));
            }
        }

        summary.append("\n").append("=".repeat(60)).append("\n");
        summary.append("相关文件:\n");
        summary.append("  - CSV详细报告: ").append(summaryFile.replace("_summary.txt", "_differences.csv")).append("\n");
        summary.append("  - JSON格式报告: ").append(summaryFile.replace("_summary.txt", "_differences.json")).append("\n");
        summary.append("  - HTML可视化报告: ").append(summaryFile.replace("_summary.txt", "_differences.html")).append("\n");

        // 写入摘要文件
        Files.write(Paths.get(summaryFile), summary.toString().getBytes("UTF-8"));
    }

    /**
     * 生成主总览报告
     */
    private void generateMasterSummary(DatabaseComparatorConfig baseDatabase,
                                       List<DatabaseComparatorConfig> targetDatabases,
                                       ComparisonResult result,
                                       LocalDateTime comparisonTime) throws Exception {

        String safeBaseName = sanitizeFileName(baseDatabase.getName());
        String masterSummaryFile = BASE_OUTPUT_DIR + "/" + safeBaseName + "/MASTER_SUMMARY.txt";

        StringBuilder summary = new StringBuilder();
        summary.append("数据库结构对比主总览报告\n");
        summary.append("=".repeat(80)).append("\n");
        summary.append("基准数据库: ").append(baseDatabase.getDisplayName()).append("\n");
        summary.append("基准Schema: ").append(baseDatabase.getSchema()).append("\n");
        summary.append("目标数据库总数: ").append(targetDatabases.size()).append("\n");
        summary.append("对比完成时间: ").append(comparisonTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))).append("\n");
        summary.append("成功对比数量: ").append(result.getSuccessCount()).append("\n");
        summary.append("失败对比数量: ").append(result.getFailureCount()).append("\n\n");

        // 对比结果统计
        summary.append("📊 对比结果概览:\n");
        summary.append("-".repeat(80)).append("\n");
        summary.append(String.format("  %-40s %10s %10s\n", "数据库名称", "状态", "差异数"));
        summary.append("-".repeat(80)).append("\n");

        for (DatabaseComparisonResult dbResult : result.getDatabaseResults()) {
            String status = dbResult.isSuccess() ? "✅ 成功" : "❌ 失败";
            String diffCount = dbResult.isSuccess() ? String.valueOf(dbResult.getDifferenceCount()) : "N/A";
            summary.append(String.format("  %-40s %10s %10s\n",
                    dbResult.getDatabase().getDisplayName(), status, diffCount));
        }

        // 问题数据库排行
        List<DatabaseComparisonResult> problemDatabases = result.getDatabaseResults().stream()
                .filter(db -> db.isSuccess() && db.getDifferenceCount() > 0)
                .sorted((a, b) -> Integer.compare(b.getDifferenceCount(), a.getDifferenceCount()))
                .limit(10)
                .toList();

        if (!problemDatabases.isEmpty()) {
            summary.append("\n🚨 需要关注的数据库 (差异数 > 0):\n");
            summary.append("-".repeat(80)).append("\n");
            for (DatabaseComparisonResult db : problemDatabases) {
                summary.append(String.format("  %-40s: %d 个差异\n",
                        db.getDatabase().getDisplayName(), db.getDifferenceCount()));
            }
        }

        // 使用说明
        summary.append("\n📁 目录结构说明:\n");
        summary.append("-".repeat(50)).append("\n");
        summary.append("  comparisons/     - 详细对比报告文件\n");
        summary.append("  summaries/       - 各数据库摘要文件\n");
        summary.append("  history/         - 历史对比记录\n");

        summary.append("\n📋 文件命名规则:\n");
        summary.append("-".repeat(50)).append("\n");
        summary.append("  基准库名_vs_目标库名_differences.csv   - CSV格式详细报告\n");
        summary.append("  基准库名_vs_目标库名_differences.json  - JSON格式报告\n");
        summary.append("  基准库名_vs_目标库名_differences.html  - HTML可视化报告\n");
        summary.append("  基准库名_vs_目标库名_summary.txt       - 快速摘要\n");

        // 写入主总览文件
        Files.write(Paths.get(masterSummaryFile), summary.toString().getBytes("UTF-8"));

        logger.info("主总览报告已生成: {}", masterSummaryFile);
    }

    /**
     * 更新对比历史记录
     */
    private void updateComparisonHistory(DatabaseComparatorConfig baseDatabase,
                                         ComparisonResult result,
                                         LocalDateTime comparisonTime) throws Exception {

        String safeBaseName = sanitizeFileName(baseDatabase.getName());
        String historyFile = BASE_OUTPUT_DIR + "/" + safeBaseName + "/history/comparison_history.log";

        String historyEntry = String.format("%s | 成功: %d, 失败: %d, 总计: %d\n",
                comparisonTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                result.getSuccessCount(),
                result.getFailureCount(),
                result.getTotalCount());

        // 追加到历史文件
        Files.createDirectories(Paths.get(historyFile).getParent());

        if (Files.exists(Paths.get(historyFile))) {
            // 追加模式
            Files.write(Paths.get(historyFile), historyEntry.getBytes("UTF-8"),
                    java.nio.file.StandardOpenOption.APPEND);
        } else {
            // 创建新文件
            String header = "对比历史记录\n" + "=".repeat(50) + "\n";
            Files.write(Paths.get(historyFile), (header + historyEntry).getBytes("UTF-8"));
        }
    }

    // 辅助方法
    private void exportDifferencesToCSV(List<SchemaDifference> differences, String filename) throws Exception {
        try (PrintWriter writer = new PrintWriter(filename, "UTF-8")) {
            writer.println("基准数据库,目标数据库,Schema,表名,项目名,差异类型,详细描述");

            for (SchemaDifference diff : differences) {
                writer.printf("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"%n",
                        escapeCsvValue(diff.getBaseDatabaseDisplayName()),
                        escapeCsvValue(diff.getTargetDatabaseDisplayName()),
                        escapeCsvValue(diff.getSchemaName() != null ? diff.getSchemaName() : "public"),
                        escapeCsvValue(diff.getTableName()),
                        escapeCsvValue(diff.getItemName()),
                        escapeCsvValue(diff.getType().getDescription()),
                        escapeCsvValue(diff.getDescription()));
            }
        }
    }

    private void shutdownExecutor(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                logger.warn("线程池未在指定时间内关闭，强制关闭");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.warn("等待线程池关闭时被中断");
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private String sanitizeFileName(String name) {
        return name.replaceAll("[^a-zA-Z0-9\u4e00-\u9fa5_-]", "_");
    }

    private String escapeCsvValue(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\"", "\"\"");
    }

    private boolean isCritical(SchemaDifference.DifferenceType type) {
        return type == SchemaDifference.DifferenceType.MISSING_TABLE ||
                type == SchemaDifference.DifferenceType.PRIMARY_KEY_DIFF;
    }

    private boolean isWarning(SchemaDifference.DifferenceType type) {
        return type == SchemaDifference.DifferenceType.MISSING_COLUMN ||
                type == SchemaDifference.DifferenceType.COLUMN_DIFF ||
                type == SchemaDifference.DifferenceType.MISSING_INDEX;
    }

    // 内部类：对比结果
    private static class ComparisonResult {
        private final List<DatabaseComparisonResult> databaseResults = new ArrayList<>();

        public void addDatabaseResult(DatabaseComparisonResult result) {
            databaseResults.add(result);
        }

        public List<DatabaseComparisonResult> getDatabaseResults() {
            return new ArrayList<>(databaseResults);
        }

        public long getSuccessCount() {
            return databaseResults.stream().mapToLong(r -> r.isSuccess() ? 1 : 0).sum();
        }

        public long getFailureCount() {
            return databaseResults.stream().mapToLong(r -> r.isSuccess() ? 0 : 1).sum();
        }

        public int getTotalCount() {
            return databaseResults.size();
        }
    }

    // 内部类：单个数据库对比结果
    @Getter
    private static class DatabaseComparisonResult {
        private final DatabaseComparatorConfig database;
        private final boolean success;
        private final int differenceCount;

        public DatabaseComparisonResult(DatabaseComparatorConfig database, boolean success, int differenceCount) {
            this.database = database;
            this.success = success;
            this.differenceCount = differenceCount;
        }

    }
}