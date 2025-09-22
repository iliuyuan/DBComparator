package org.kingdee.dbcompare.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.kingdee.dbcompare.model.SchemaDifference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class EnhancedReportGenerator {

    private static final Logger logger = LoggerFactory.getLogger(EnhancedReportGenerator.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 生成多层级报告结构
     *
     * @param differences   所有差异数据
     * @param baseOutputDir 基础输出目录
     * @param timestamp     时间戳
     */
    public ReportResult generateMultiLevelReports(List<SchemaDifference> differences,
                                                  String baseOutputDir,
                                                  String timestamp) throws IOException {

        logger.info("开始生成多层级报告，共 {} 个差异，涉及 {} 个目标数据库",
                differences.size(),
                differences.stream().map(SchemaDifference::getTargetDatabaseName).distinct().count());

        // 创建报告目录结构
        ReportPaths paths = createReportStructure(baseOutputDir, timestamp);
        ReportResult result = new ReportResult();

        // 1. 生成总览报告
        generateSummaryReports(differences, paths, result);

        // 2. 生成按数据库分组的详细报告
        generateDatabaseSpecificReports(differences, paths, result);

        // 3. 生成按差异类型分组的报告
        generateTypeSpecificReports(differences, paths, result);

        // 4. 生成索引文件
        generateIndexFile(differences, paths, result);

        logger.info("报告生成完成，总计生成 {} 个文件", result.getTotalFiles());
        return result;
    }

    /**
     * 创建报告目录结构
     */
    private ReportPaths createReportStructure(String baseOutputDir, String timestamp) throws IOException {
        ReportPaths paths = new ReportPaths();

        // 主报告目录
        String mainDir = baseOutputDir + "/schema_compare_" + timestamp;
        paths.mainDir = Paths.get(mainDir);
        Files.createDirectories(paths.mainDir);

        // 子目录
        paths.summaryDir = paths.mainDir.resolve("summary");
        paths.databasesDir = paths.mainDir.resolve("by_database");
        paths.typesDir = paths.mainDir.resolve("by_type");

        Files.createDirectories(paths.summaryDir);
        Files.createDirectories(paths.databasesDir);
        Files.createDirectories(paths.typesDir);

        logger.info("创建报告目录结构: {}", paths.mainDir.toAbsolutePath());
        return paths;
    }

    /**
     * 生成总览报告
     */
    private void generateSummaryReports(List<SchemaDifference> differences,
                                        ReportPaths paths,
                                        ReportResult result) throws IOException {

        logger.info("生成总览报告...");

        // 统计信息
        Map<String, Object> summary = buildSummaryStatistics(differences);

        // CSV格式总览
        String csvFile = paths.summaryDir.resolve("summary_overview.csv").toString();
        generateSummaryCSV(differences, csvFile, summary);
        result.addFile("总览CSV报告", csvFile);

        // JSON格式总览
        String jsonFile = paths.summaryDir.resolve("summary_overview.json").toString();
        generateSummaryJSON(summary, jsonFile);
        result.addFile("总览JSON报告", jsonFile);

        // HTML格式总览
        String htmlFile = paths.summaryDir.resolve("summary_overview.html").toString();
        generateSummaryHTML(differences, htmlFile, summary);
        result.addFile("总览HTML报告", htmlFile);
    }

    /**
     * 生成按数据库分组的详细报告
     */
    private void generateDatabaseSpecificReports(List<SchemaDifference> differences,
                                                 ReportPaths paths,
                                                 ReportResult result) throws IOException {

        logger.info("生成数据库专项报告...");

        // 按目标数据库分组
        Map<String, List<SchemaDifference>> byDatabase = differences.stream()
                .collect(Collectors.groupingBy(SchemaDifference::getTargetDatabaseName));

        int dbCount = 0;
        for (Map.Entry<String, List<SchemaDifference>> entry : byDatabase.entrySet()) {
            String dbName = entry.getKey();
            List<SchemaDifference> dbDifferences = entry.getValue();

            // 为每个数据库创建子目录
            Path dbDir = paths.databasesDir.resolve(sanitizeFileName(dbName));
            Files.createDirectories(dbDir);

            // 生成该数据库的详细报告
            generateDatabaseReport(dbDifferences, dbDir, dbName, result);
            dbCount++;

            if (dbCount % 10 == 0) {
                logger.info("已处理 {}/{} 个数据库报告", dbCount, byDatabase.size());
            }
        }

        logger.info("数据库专项报告生成完成，共 {} 个数据库", byDatabase.size());
    }

    /**
     * 为单个数据库生成详细报告
     */
    private void generateDatabaseReport(List<SchemaDifference> differences,
                                        Path dbDir,
                                        String dbName,
                                        ReportResult result) throws IOException {

        String safeDbName = sanitizeFileName(dbName);

        // CSV详细报告
        String csvFile = dbDir.resolve(safeDbName + "_details.csv").toString();
        generateDetailedCSV(differences, csvFile);
        result.addFile(dbName + " - 详细CSV", csvFile);

        // JSON详细报告
        String jsonFile = dbDir.resolve(safeDbName + "_details.json").toString();
        generateDetailedJSON(differences, jsonFile);
        result.addFile(dbName + " - 详细JSON", jsonFile);

        // HTML详细报告
        String htmlFile = dbDir.resolve(safeDbName + "_details.html").toString();
        generateDetailedHTML(differences, htmlFile, dbName);
        result.addFile(dbName + " - 详细HTML", htmlFile);
    }

    /**
     * 生成按差异类型分组的报告
     */
    private void generateTypeSpecificReports(List<SchemaDifference> differences,
                                             ReportPaths paths,
                                             ReportResult result) throws IOException {

        logger.info("生成差异类型专项报告...");

        // 按差异类型分组
        Map<SchemaDifference.DifferenceType, List<SchemaDifference>> byType = differences.stream()
                .collect(Collectors.groupingBy(SchemaDifference::getType));

        for (Map.Entry<SchemaDifference.DifferenceType, List<SchemaDifference>> entry : byType.entrySet()) {
            SchemaDifference.DifferenceType type = entry.getKey();
            List<SchemaDifference> typeDifferences = entry.getValue();

            String typeFileName = sanitizeFileName(type.getDescription());

            // CSV报告
            String csvFile = paths.typesDir.resolve(typeFileName + ".csv").toString();
            generateTypeSpecificCSV(typeDifferences, csvFile, type);
            result.addFile(type.getDescription() + " - CSV", csvFile);

            // JSON报告
            String jsonFile = paths.typesDir.resolve(typeFileName + ".json").toString();
            generateTypeSpecificJSON(typeDifferences, jsonFile, type);
            result.addFile(type.getDescription() + " - JSON", jsonFile);
        }
    }

    /**
     * 生成索引文件
     */
    private void generateIndexFile(List<SchemaDifference> differences,
                                   ReportPaths paths,
                                   ReportResult result) throws IOException {

        logger.info("生成索引文件...");

        String indexFile = paths.mainDir.resolve("index.html").toString();
        generateIndexHTML(differences, indexFile, result);
        result.setIndexFile(indexFile);
    }

    /**
     * 构建统计信息
     */
    private Map<String, Object> buildSummaryStatistics(List<SchemaDifference> differences) {
        Map<String, Object> summary = new HashMap<>();

        summary.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        summary.put("totalDifferences", differences.size());

        // 按数据库统计
        Map<String, Long> dbStats = differences.stream()
                .collect(Collectors.groupingBy(
                        SchemaDifference::getTargetDatabaseDisplayName,
                        Collectors.counting()
                ));
        summary.put("databaseCount", dbStats.size());
        summary.put("databaseStatistics", dbStats);

        // 按类型统计
        Map<String, Long> typeStats = differences.stream()
                .collect(Collectors.groupingBy(
                        diff -> diff.getType().getDescription(),
                        Collectors.counting()
                ));
        summary.put("typeStatistics", typeStats);

        // 问题严重性分析
        Map<String, Object> severity = analyzeSeverity(differences);
        summary.put("severityAnalysis", severity);

        // 基准数据库信息
        if (!differences.isEmpty()) {
            SchemaDifference first = differences.get(0);
            Map<String, String> baseInfo = new HashMap<>();
            baseInfo.put("name", first.getBaseDatabaseName());
            baseInfo.put("displayName", first.getBaseDatabaseDisplayName());
            baseInfo.put("schema", first.getSchemaName());
            summary.put("baseDatabaseInfo", baseInfo);
        }

        return summary;
    }

    /**
     * 分析差异严重性
     */
    private Map<String, Object> analyzeSeverity(List<SchemaDifference> differences) {
        Map<String, Object> severity = new HashMap<>();

        long critical = differences.stream()
                .mapToLong(diff -> isCriticalDifference(diff.getType()) ? 1 : 0)
                .sum();

        long warning = differences.stream()
                .mapToLong(diff -> isWarningDifference(diff.getType()) ? 1 : 0)
                .sum();

        long info = differences.size() - critical - warning;

        severity.put("critical", critical);
        severity.put("warning", warning);
        severity.put("info", info);

        // 问题数据库列表（差异数量超过阈值的）
        List<String> problemDatabases = differences.stream()
                .collect(Collectors.groupingBy(
                        SchemaDifference::getTargetDatabaseDisplayName,
                        Collectors.counting()
                ))
                .entrySet().stream()
                .filter(entry -> entry.getValue() > 10) // 差异超过10个认为是问题数据库
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        severity.put("problemDatabases", problemDatabases);

        return severity;
    }

    /**
     * 判断是否为严重差异
     */
    private boolean isCriticalDifference(SchemaDifference.DifferenceType type) {
        return type == SchemaDifference.DifferenceType.MISSING_TABLE ||
                type == SchemaDifference.DifferenceType.PRIMARY_KEY_DIFF;
    }

    /**
     * 判断是否为警告级别差异
     */
    private boolean isWarningDifference(SchemaDifference.DifferenceType type) {
        return type == SchemaDifference.DifferenceType.MISSING_COLUMN ||
                type == SchemaDifference.DifferenceType.COLUMN_DIFF ||
                type == SchemaDifference.DifferenceType.MISSING_INDEX;
    }

    /**
     * 生成总览CSV报告
     */
    private void generateSummaryCSV(List<SchemaDifference> differences,
                                    String filename,
                                    Map<String, Object> summary) throws IOException {

        try (FileWriter writer = new FileWriter(filename);
             CSVPrinter csvPrinter = new CSVPrinter(writer,
                     CSVFormat.DEFAULT.withHeader("数据库", "差异总数", "严重", "警告", "一般", "主要问题类型"))) {

            Map<String, List<SchemaDifference>> byDatabase = differences.stream()
                    .collect(Collectors.groupingBy(SchemaDifference::getTargetDatabaseDisplayName));

            for (Map.Entry<String, List<SchemaDifference>> entry : byDatabase.entrySet()) {
                String dbName = entry.getKey();
                List<SchemaDifference> dbDiffs = entry.getValue();

                long critical = dbDiffs.stream()
                        .mapToLong(diff -> isCriticalDifference(diff.getType()) ? 1 : 0)
                        .sum();

                long warning = dbDiffs.stream()
                        .mapToLong(diff -> isWarningDifference(diff.getType()) ? 1 : 0)
                        .sum();

                long info = dbDiffs.size() - critical - warning;

                String mainProblem = dbDiffs.stream()
                        .collect(Collectors.groupingBy(
                                diff -> diff.getType().getDescription(),
                                Collectors.counting()
                        ))
                        .entrySet().stream()
                        .max(Map.Entry.comparingByValue())
                        .map(Map.Entry::getKey)
                        .orElse("无");

                csvPrinter.printRecord(dbName, dbDiffs.size(), critical, warning, info, mainProblem);
            }
        }
    }

    /**
     * 生成总览JSON报告
     */
    private void generateSummaryJSON(Map<String, Object> summary, String filename) throws IOException {
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(filename), summary);
    }

    /**
     * 生成总览HTML报告
     */
    private void generateSummaryHTML(List<SchemaDifference> differences,
                                     String filename,
                                     Map<String, Object> summary) throws IOException {

        StringBuilder html = new StringBuilder();
        html.append(getHTMLHeader("数据库结构差异总览报告"));

        // 总体统计
        html.append("<div class='summary-card'>\n")
                .append("<h2>📊 总体统计</h2>\n")
                .append("<div class='stats-grid'>\n")
                .append("<div class='stat-item critical'><div class='stat-number'>")
                .append(differences.size()).append("</div><div class='stat-label'>总差异数</div></div>\n")
                .append("<div class='stat-item'><div class='stat-number'>")
                .append(summary.get("databaseCount")).append("</div><div class='stat-label'>涉及数据库</div></div>\n");

        @SuppressWarnings("unchecked")
        Map<String, Object> severity = (Map<String, Object>) summary.get("severityAnalysis");
        html.append("<div class='stat-item critical'><div class='stat-number'>")
                .append(severity.get("critical")).append("</div><div class='stat-label'>严重问题</div></div>\n")
                .append("<div class='stat-item warning'><div class='stat-number'>")
                .append(severity.get("warning")).append("</div><div class='stat-label'>警告问题</div></div>\n")
                .append("</div></div>\n");

        // 数据库排行榜（问题最多的数据库）
        html.append(generateDatabaseRanking(differences));

        // 问题类型分布
        html.append(generateTypeDistribution(differences));

        html.append(getHTMLFooter());

        try (FileWriter writer = new FileWriter(filename)) {
            writer.write(html.toString());
        }
    }

    /**
     * 生成详细CSV报告
     */
    private void generateDetailedCSV(List<SchemaDifference> differences, String filename) throws IOException {
        try (FileWriter writer = new FileWriter(filename);
             CSVPrinter csvPrinter = new CSVPrinter(writer,
                     CSVFormat.DEFAULT.withHeader("基准数据库", "目标数据库", "Schema", "表名", "项目名", "差异类型", "详细描述", "基准值", "目标值"))) {

            for (SchemaDifference diff : differences) {
                csvPrinter.printRecord(
                        diff.getBaseDatabaseDisplayName(),
                        diff.getTargetDatabaseDisplayName(),
                        diff.getSchemaName(),
                        diff.getTableName(),
                        diff.getItemName(),
                        diff.getType().getDescription(),
                        diff.getDescription(),
                        diff.getBaseValue() != null ? diff.getBaseValue() : "",
                        diff.getTargetValue() != null ? diff.getTargetValue() : ""
                );
            }
        }
    }

    /**
     * 生成详细JSON报告
     */
    private void generateDetailedJSON(List<SchemaDifference> differences, String filename) throws IOException {
        Map<String, Object> report = new HashMap<>();
        report.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        report.put("totalDifferences", differences.size());
        report.put("differences", differences.stream().map(this::convertToJsonMap).collect(Collectors.toList()));

        objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(filename), report);
    }

    /**
     * 生成详细HTML报告
     */
    private void generateDetailedHTML(List<SchemaDifference> differences, String filename, String dbName) throws IOException {
        StringBuilder html = new StringBuilder();
        html.append(getHTMLHeader("数据库详细差异报告 - " + dbName));

        // 数据库信息
        html.append("<div class='db-info'>\n")
                .append("<h2>🗄️ 数据库信息</h2>\n")
                .append("<p><strong>目标数据库:</strong> ").append(escapeHtml(dbName)).append("</p>\n");

        if (!differences.isEmpty()) {
            SchemaDifference first = differences.get(0);
            html.append("<p><strong>基准数据库:</strong> ").append(escapeHtml(first.getBaseDatabaseDisplayName())).append("</p>\n")
                    .append("<p><strong>Schema:</strong> ").append(escapeHtml(first.getSchemaName())).append("</p>\n");
        }

        html.append("<p><strong>差异总数:</strong> ").append(differences.size()).append("</p>\n")
                .append("</div>\n");

        // 差异表格
        html.append(generateDifferenceTable(differences));
        html.append(getHTMLFooter());

        try (FileWriter writer = new FileWriter(filename)) {
            writer.write(html.toString());
        }
    }

    // ... 其他辅助方法 ...

    /**
     * 安全的文件名处理
     */
    private String sanitizeFileName(String name) {
        return name.replaceAll("[^a-zA-Z0-9\u4e00-\u9fa5_-]", "_");
    }

    /**
     * 转换为JSON映射
     */
    private Map<String, Object> convertToJsonMap(SchemaDifference diff) {
        Map<String, Object> map = new HashMap<>();
        map.put("baseDatabaseName", diff.getBaseDatabaseName());
        map.put("targetDatabaseName", diff.getTargetDatabaseName());
        map.put("baseDatabaseDisplayName", diff.getBaseDatabaseDisplayName());
        map.put("targetDatabaseDisplayName", diff.getTargetDatabaseDisplayName());
        map.put("schemaName", diff.getSchemaName());
        map.put("tableName", diff.getTableName());
        map.put("itemName", diff.getItemName());
        map.put("type", diff.getType().toString());
        map.put("typeDescription", diff.getType().getDescription());
        map.put("description", diff.getDescription());

        if (diff.getBaseValue() != null) {
            map.put("baseValue", diff.getBaseValue());
        }
        if (diff.getTargetValue() != null) {
            map.put("targetValue", diff.getTargetValue());
        }

        return map;
    }

    // HTML生成辅助方法
    private String getHTMLHeader(String title) {
        return "<!DOCTYPE html>\n<html lang='zh-CN'>\n<head>\n" +
                "<meta charset='UTF-8'>\n<title>" + title + "</title>\n" +
                "<style>\n" + getHTMLStyles() + "</style>\n</head>\n<body>\n" +
                "<div class='container'>\n<h1>" + title + "</h1>\n";
    }

    private String getHTMLFooter() {
        return "</div>\n<script>" + getJavaScript() + "</script>\n</body>\n</html>";
    }

    private String getHTMLStyles() {
        return """
                body { font-family: 'Microsoft YaHei', Arial, sans-serif; margin: 0; background: #f5f7fa; }
                .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
                h1 { color: #2c3e50; text-align: center; margin-bottom: 30px; }
                .summary-card { background: white; padding: 25px; margin: 20px 0; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-top: 20px; }
                .stat-item { text-align: center; padding: 20px; border-radius: 8px; background: #ecf0f1; }
                .stat-item.critical { background: #ffe6e6; color: #c0392b; }
                .stat-item.warning { background: #fff3cd; color: #856404; }
                .stat-number { font-size: 2.5em; font-weight: bold; margin-bottom: 5px; }
                .stat-label { font-size: 0.9em; opacity: 0.8; }
                table { width: 100%; border-collapse: collapse; margin-top: 20px; background: white; }
                th, td { padding: 12px 8px; text-align: left; border-bottom: 1px solid #ddd; }
                th { background: #34495e; color: white; font-weight: 600; position: sticky; top: 0; }
                .type-badge { padding: 4px 8px; border-radius: 4px; color: white; font-size: 0.85em; }
                .type-missing { background: #e74c3c; }
                .type-extra { background: #27ae60; }
                .type-different { background: #f39c12; }
                """;
    }

    private String getJavaScript() {
        return """
                // 表格排序功能
                document.querySelectorAll('th').forEach(header => {
                    header.style.cursor = 'pointer';
                    header.addEventListener('click', () => sortTable(header));
                });
                
                function sortTable(header) {
                    // 简单的表格排序实现
                    console.log('排序列:', header.textContent);
                }
                """;
    }

    private String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }

    // 省略其他辅助方法的实现...
    private String generateDatabaseRanking(List<SchemaDifference> differences) {
        return "";
    }

    private String generateTypeDistribution(List<SchemaDifference> differences) {
        return "";
    }

    private String generateDifferenceTable(List<SchemaDifference> differences) {
        return "";
    }

    private void generateTypeSpecificCSV(List<SchemaDifference> diffs, String file, SchemaDifference.DifferenceType type) throws IOException {
    }

    private void generateTypeSpecificJSON(List<SchemaDifference> diffs, String file, SchemaDifference.DifferenceType type) throws IOException {
    }

    private void generateIndexHTML(List<SchemaDifference> diffs, String file, ReportResult result) throws IOException {
    }

    // 内部类
    private static class ReportPaths {
        Path mainDir;
        Path summaryDir;
        Path databasesDir;
        Path typesDir;
    }

    public static class ReportResult {
        private final Map<String, String> files = new HashMap<>();
        private String indexFile;

        public void addFile(String description, String path) {
            files.put(description, path);
        }

        public void setIndexFile(String indexFile) {
            this.indexFile = indexFile;
        }

        public int getTotalFiles() {
            return files.size() + (indexFile != null ? 1 : 0);
        }

        public Map<String, String> getFiles() {
            return new HashMap<>(files);
        }

        public String getIndexFile() {
            return indexFile;
        }
    }
}