package org.kingdee.dbcompare.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.kingdee.dbcompare.model.SchemaDifference;
import org.springframework.stereotype.Service;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ReportGeneratorService {

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 导出到CSV格式（优化为显示IP信息）
     */
    public void exportToCSV(List<SchemaDifference> differences, String filename) throws IOException {
        try (FileWriter writer = new FileWriter(filename);
             CSVPrinter csvPrinter = new CSVPrinter(writer,
                     CSVFormat.DEFAULT.withHeader("基准数据库", "目标数据库", "Schema", "表名", "项目名", "差异类型", "描述", "时间戳"))) {

            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            for (SchemaDifference diff : differences) {
                csvPrinter.printRecord(
                        diff.getBaseDatabaseDisplayName(),     // 使用包含IP的显示名称
                        diff.getTargetDatabaseDisplayName(),   // 使用包含IP的显示名称
                        diff.getSchemaName(),
                        diff.getTableName(),
                        diff.getItemName(),
                        diff.getType().getDescription(),
                        diff.getDescription(),
                        timestamp
                );
            }
        }
    }

    /**
     * 导出到JSON格式（优化为显示IP信息）
     */
    public void exportToJSON(List<SchemaDifference> differences, String filename) throws IOException {
        Map<String, Object> report = new HashMap<>();
        report.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        report.put("totalDifferences", differences.size());

        // 构建增强的差异信息，包含IP显示
        List<Map<String, Object>> enhancedDifferences = differences.stream()
                .map(this::convertToJsonMap)
                .collect(java.util.stream.Collectors.toList());

        report.put("differences", enhancedDifferences);

        // 按类型统计
        Map<String, Long> typeStats = differences.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        diff -> diff.getType().toString(),
                        java.util.stream.Collectors.counting()
                ));
        report.put("statisticsByType", typeStats);

        // 按数据库统计（使用显示名称，包含IP）
        Map<String, Long> dbStats = differences.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        SchemaDifference::getTargetDatabaseDisplayName,
                        java.util.stream.Collectors.counting()
                ));
        report.put("statisticsByDatabase", dbStats);

        // 添加基准数据库信息
        if (!differences.isEmpty()) {
            SchemaDifference firstDiff = differences.get(0);
            Map<String, String> baseDatabaseInfo = new HashMap<>();
            baseDatabaseInfo.put("name", firstDiff.getBaseDatabaseName());
            baseDatabaseInfo.put("displayName", firstDiff.getBaseDatabaseDisplayName());
            baseDatabaseInfo.put("schema", firstDiff.getSchemaName());
            report.put("baseDatabaseInfo", baseDatabaseInfo);
        }

        objectMapper.writerWithDefaultPrettyPrinter().writeValue(new java.io.File(filename), report);
    }

    /**
     * 将SchemaDifference转换为JSON Map，包含IP显示信息
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

    /**
     * 优化的HTML报告生成方法 - 减少冗余信息，增强可读性
     */
    public void exportToHTML(List<SchemaDifference> differences, String filename) throws IOException {
        StringBuilder html = new StringBuilder();

        html.append("<!DOCTYPE html>\n")
                .append("<html lang='zh-CN'>\n")
                .append("<head>\n")
                .append("    <meta charset='UTF-8'>\n")
                .append("    <meta name='viewport' content='width=device-width, initial-scale=1.0'>\n")
                .append("    <title>数据库结构差异报告</title>\n")
                .append("    <style>\n")
                .append(generateOptimizedCSS())
                .append("    </style>\n")
                .append("</head>\n")
                .append("<body>\n")
                .append("<div class='container'>\n");

        // 报告标题
        html.append("<h1>数据库结构差异报告</h1>\n");

        if (differences.isEmpty()) {
            html.append("<div class='success-message'>\n")
                    .append("    <h2>✅ 完全一致</h2>\n")
                    .append("    <p>所有数据库结构完全一致，未发现任何差异。</p>\n")
                    .append("</div>\n");
        } else {
            // 对比信息概览
            generateComparisonOverview(html, differences);

            // 统计概览
            generateStatisticsOverview(html, differences);

            // 按数据库分组的差异报告
            generateGroupedDifferenceReport(html, differences);
        }

        html.append("</div>\n")
                .append("</body>\n")
                .append("</html>");

        try (FileWriter writer = new FileWriter(filename)) {
            writer.write(html.toString());
        }
    }

    /**
     * 生成优化的CSS样式
     */
    private String generateOptimizedCSS() {
        return """
                body { 
                    font-family: 'Microsoft YaHei', -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; 
                    margin: 0; 
                    padding: 20px; 
                    background-color: #f8fafc; 
                    color: #333; 
                    line-height: 1.6;
                }
                
                .container { 
                    max-width: 1200px; 
                    margin: 0 auto; 
                    background: white; 
                    padding: 40px; 
                    border-radius: 12px; 
                    box-shadow: 0 4px 20px rgba(0,0,0,0.08);
                }
                
                h1 { 
                    color: #2563eb; 
                    text-align: center; 
                    margin-bottom: 40px; 
                    font-size: 2.5em;
                    font-weight: 300;
                }
                
                .comparison-overview {
                    display: grid;
                    grid-template-columns: 1fr 1fr;
                    gap: 30px;
                    margin-bottom: 40px;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 30px;
                    border-radius: 12px;
                }
                
                .db-info {
                    background: rgba(255,255,255,0.1);
                    padding: 20px;
                    border-radius: 8px;
                    backdrop-filter: blur(10px);
                }
                
                .db-info h3 {
                    margin: 0 0 15px 0;
                    font-size: 1.2em;
                    opacity: 0.9;
                }
                
                .db-detail {
                    font-size: 1.1em;
                    margin: 5px 0;
                }
                
                .stats-grid { 
                    display: grid; 
                    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); 
                    gap: 20px; 
                    margin: 30px 0; 
                }
                
                .stat-card { 
                    background: #f8fafc;
                    border: 2px solid #e2e8f0;
                    border-radius: 8px; 
                    padding: 20px; 
                    text-align: center;
                    transition: transform 0.2s;
                }
                
                .stat-card:hover {
                    transform: translateY(-2px);
                    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
                }
                
                .stat-number { 
                    font-size: 2.5em; 
                    font-weight: bold; 
                    margin-bottom: 5px;
                }
                
                .stat-number.critical { color: #dc2626; }
                .stat-number.warning { color: #ea580c; }
                .stat-number.info { color: #2563eb; }
                
                .stat-label { 
                    color: #64748b; 
                    font-size: 0.9em;
                    font-weight: 500;
                }
                
                .database-section {
                    margin: 40px 0;
                    border: 1px solid #e2e8f0;
                    border-radius: 12px;
                    overflow: hidden;
                }
                
                .database-header {
                    background: #f1f5f9;
                    padding: 20px 30px;
                    border-bottom: 1px solid #e2e8f0;
                }
                
                .database-title {
                    font-size: 1.4em;
                    font-weight: 600;
                    color: #1e293b;
                    margin: 0 0 10px 0;
                }
                
                .database-stats {
                    display: flex;
                    gap: 30px;
                    font-size: 0.95em;
                    color: #64748b;
                }
                
                .database-stats span {
                    font-weight: 600;
                    color: #374151;
                }
                
                .differences-table {
                    width: 100%;
                    border-collapse: collapse;
                    margin: 0;
                }
                
                .differences-table th {
                    background: #f8fafc;
                    padding: 15px 20px;
                    text-align: left;
                    font-weight: 600;
                    color: #374151;
                    border-bottom: 2px solid #e2e8f0;
                    position: sticky;
                    top: 0;
                    z-index: 10;
                }
                
                .differences-table td {
                    padding: 12px 20px;
                    border-bottom: 1px solid #f1f5f9;
                    vertical-align: top;
                }
                
                .differences-table tr:hover {
                    background: #fafbfc;
                }
                
                .type-badge { 
                    padding: 6px 12px; 
                    border-radius: 20px; 
                    color: white; 
                    font-size: 0.8em; 
                    font-weight: 600;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                }
                
                .type-missing { background: linear-gradient(45deg, #dc2626, #ef4444); }
                .type-extra { background: linear-gradient(45deg, #16a34a, #22c55e); }
                .type-different { background: linear-gradient(45deg, #ea580c, #f97316); }
                
                .table-name {
                    font-weight: 600;
                    color: #1e293b;
                }
                
                .item-name {
                    font-family: 'Consolas', 'Monaco', monospace;
                    background: #f1f5f9;
                    padding: 2px 6px;
                    border-radius: 4px;
                    font-size: 0.9em;
                }
                
                .description {
                    max-width: 400px;
                    word-wrap: break-word;
                    color: #64748b;
                    font-size: 0.95em;
                }
                
                .success-message {
                    text-align: center;
                    background: linear-gradient(135deg, #10b981, #34d399);
                    color: white;
                    padding: 40px;
                    border-radius: 12px;
                    margin: 40px 0;
                }
                
                .success-message h2 {
                    margin: 0 0 15px 0;
                    font-size: 2em;
                }
                
                .no-differences {
                    text-align: center;
                    background: #f0fdf4;
                    color: #166534;
                    padding: 30px;
                    border-radius: 8px;
                    border: 1px solid #bbf7d0;
                }
                
                @media (max-width: 768px) {
                    .comparison-overview {
                        grid-template-columns: 1fr;
                        gap: 20px;
                    }
                
                    .stats-grid {
                        grid-template-columns: repeat(2, 1fr);
                    }
                
                    .differences-table {
                        font-size: 0.9em;
                    }
                }
                """;
    }

    /**
     * 生成对比信息概览
     */
    private void generateComparisonOverview(StringBuilder html, List<SchemaDifference> differences) {
        if (differences.isEmpty()) return;

        SchemaDifference firstDiff = differences.get(0);
        String generateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        html.append("<div class='comparison-overview'>\n")
                .append("    <div class='db-info'>\n")
                .append("        <h3>📊 基准数据库</h3>\n")
                .append("        <div class='db-detail'><strong>数据库：</strong>")
                .append(escapeHtml(firstDiff.getBaseDatabaseDisplayName())).append("</div>\n")
                .append("        <div class='db-detail'><strong>Schema：</strong>")
                .append(escapeHtml(firstDiff.getSchemaName())).append("</div>\n")
                .append("    </div>\n")
                .append("    <div class='db-info'>\n")
                .append("        <h3>📈 对比概要</h3>\n")
                .append("        <div class='db-detail'><strong>生成时间：</strong>").append(generateTime).append("</div>\n")
                .append("        <div class='db-detail'><strong>总差异数：</strong>").append(differences.size()).append("</div>\n")
                .append("        <div class='db-detail'><strong>涉及数据库：</strong>")
                .append(getDatabaseCount(differences)).append(" 个</div>\n")
                .append("    </div>\n")
                .append("</div>\n");
    }

    /**
     * 生成统计概览
     */
    private void generateStatisticsOverview(StringBuilder html, List<SchemaDifference> differences) {
        Map<String, Long> typeStats = differences.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        diff -> diff.getType().getDescription(),
                        java.util.LinkedHashMap::new,
                        java.util.stream.Collectors.counting()
                ));

        html.append("<div class='stats-grid'>\n");

        // 总差异数
        html.append("<div class='stat-card'>\n")
                .append("    <div class='stat-number critical'>").append(differences.size()).append("</div>\n")
                .append("    <div class='stat-label'>总差异数</div>\n")
                .append("</div>\n");

        // 各类型统计
        for (Map.Entry<String, Long> entry : typeStats.entrySet()) {
            String cssClass = getStatCssClass(entry.getKey());
            html.append("<div class='stat-card'>\n")
                    .append("    <div class='stat-number ").append(cssClass).append("'>")
                    .append(entry.getValue()).append("</div>\n")
                    .append("    <div class='stat-label'>").append(entry.getKey()).append("</div>\n")
                    .append("</div>\n");
        }

        html.append("</div>\n");
    }

    /**
     * 生成按数据库分组的差异报告
     */
    private void generateGroupedDifferenceReport(StringBuilder html, List<SchemaDifference> differences) {
        // 按目标数据库分组
        Map<String, List<SchemaDifference>> byDatabase = differences.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        SchemaDifference::getTargetDatabaseDisplayName,
                        java.util.LinkedHashMap::new,
                        java.util.stream.Collectors.toList()
                ));

        for (Map.Entry<String, List<SchemaDifference>> entry : byDatabase.entrySet()) {
            String targetDbName = entry.getKey();
            List<SchemaDifference> dbDiffs = entry.getValue();

            html.append("<div class='database-section'>\n");

            // 数据库头部信息
            generateDatabaseHeader(html, targetDbName, dbDiffs);

            if (dbDiffs.isEmpty()) {
                html.append("<div class='no-differences'>\n")
                        .append("    <p>✅ 该数据库与基准数据库结构完全一致，未发现差异。</p>\n")
                        .append("</div>\n");
            } else {
                // 差异表格
                generateDifferencesTable(html, dbDiffs);
            }

            html.append("</div>\n");
        }
    }

    /**
     * 生成数据库头部信息
     */
    private void generateDatabaseHeader(StringBuilder html, String targetDbName, List<SchemaDifference> dbDiffs) {
        // 统计各类型差异
        Map<String, Long> typeStats = dbDiffs.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        diff -> diff.getType().getDescription(),
                        java.util.stream.Collectors.counting()
                ));

        // 获取Schema信息
        String schemaName = dbDiffs.isEmpty() ? "N/A" : dbDiffs.get(0).getSchemaName();

        html.append("<div class='database-header'>\n")
                .append("    <div class='database-title'>🎯 目标数据库: ").append(escapeHtml(targetDbName)).append("</div>\n")
                .append("    <div class='database-stats'>\n")
                .append("        <div>Schema: <span>").append(escapeHtml(schemaName)).append("</span></div>\n")
                .append("        <div>总差异: <span>").append(dbDiffs.size()).append("</span></div>\n");

        // 显示主要差异类型统计
        typeStats.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(3)
                .forEach(stat ->
                        html.append("        <div>").append(stat.getKey()).append(": <span>")
                                .append(stat.getValue()).append("</span></div>\n")
                );

        html.append("    </div>\n")
                .append("</div>\n");
    }

    /**
     * 生成差异表格（简化版，去除冗余列）
     */
    private void generateDifferencesTable(StringBuilder html, List<SchemaDifference> dbDiffs) {
        html.append("<table class='differences-table'>\n")
                .append("    <thead>\n")
                .append("        <tr>\n")
                .append("            <th style='width: 15%'>表名</th>\n")
                .append("            <th style='width: 20%'>项目名</th>\n")
                .append("            <th style='width: 15%'>差异类型</th>\n")
                .append("            <th style='width: 50%'>详细描述</th>\n")
                .append("        </tr>\n")
                .append("    </thead>\n")
                .append("    <tbody>\n");

        // 按表名分组并排序
        Map<String, List<SchemaDifference>> byTable = dbDiffs.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        SchemaDifference::getTableName,
                        java.util.LinkedHashMap::new,
                        java.util.stream.Collectors.toList()
                ));

        for (Map.Entry<String, List<SchemaDifference>> tableEntry : byTable.entrySet()) {
            List<SchemaDifference> tableDiffs = tableEntry.getValue();

            // 按差异类型排序（严重程度）
            tableDiffs.sort((a, b) -> getSeverityOrder(a.getType()) - getSeverityOrder(b.getType()));

            for (SchemaDifference diff : tableDiffs) {
                String typeBadgeClass = getTypeBadgeClass(diff.getType());

                html.append("        <tr>\n")
                        .append("            <td><span class='table-name'>")
                        .append(escapeHtml(diff.getTableName())).append("</span></td>\n")
                        .append("            <td><span class='item-name'>")
                        .append(escapeHtml(diff.getItemName())).append("</span></td>\n")
                        .append("            <td><span class='type-badge ").append(typeBadgeClass).append("'>")
                        .append(diff.getType().getDescription()).append("</span></td>\n")
                        .append("            <td><div class='description'>")
                        .append(escapeHtml(diff.getDescription())).append("</div></td>\n")
                        .append("        </tr>\n");
            }
        }

        html.append("    </tbody>\n")
                .append("</table>\n");
    }

    /**
     * 获取统计数字的CSS类
     */
    private String getStatCssClass(String typeName) {
        if (typeName.contains("缺少") || typeName.contains("主键")) {
            return "critical";
        } else if (typeName.contains("不同") || typeName.contains("差异")) {
            return "warning";
        }
        return "info";
    }

    /**
     * 获取差异类型的严重程度排序
     */
    private int getSeverityOrder(SchemaDifference.DifferenceType type) {
        return switch (type) {
            case MISSING_TABLE -> 1;
            case PRIMARY_KEY_DIFF -> 2;
            case MISSING_COLUMN -> 3;
            case COLUMN_DIFF -> 4;
            case MISSING_INDEX -> 5;
            case INDEX_DIFF -> 6;
            case EXTRA_TABLE -> 7;
            case EXTRA_COLUMN -> 8;
            case EXTRA_INDEX -> 9;
            default -> 10;
        };
    }

    /**
     * 获取数据库数量
     */
    private long getDatabaseCount(List<SchemaDifference> differences) {
        return differences.stream()
                .map(SchemaDifference::getTargetDatabaseDisplayName)
                .distinct()
                .count();
    }

    private String getTypeBadgeClass(SchemaDifference.DifferenceType type) {
        return switch (type) {
            case MISSING_TABLE, MISSING_COLUMN, MISSING_INDEX -> "type-missing";
            case EXTRA_TABLE, EXTRA_COLUMN, EXTRA_INDEX -> "type-extra";
            default -> "type-different";
        };
    }

    private String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }
}