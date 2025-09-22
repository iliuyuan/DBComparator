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
public class ReportGenerator {

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
     * 生成HTML报告（优化为显示IP信息）
     */
    public void exportToHTML(List<SchemaDifference> differences, String filename) throws IOException {
        StringBuilder html = new StringBuilder();

        html.append("<!DOCTYPE html>\n")
                .append("<html lang='zh-CN'>\n")
                .append("<head>\n")
                .append("    <meta charset='UTF-8'>\n")
                .append("    <title>数据库结构差异报告</title>\n")
                .append("    <style>\n")
                .append("        body { font-family: 'Microsoft YaHei', Arial, sans-serif; margin: 40px; background-color: #f5f5f5; }\n")
                .append("        .container { background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }\n")
                .append("        h1 { color: #333; text-align: center; margin-bottom: 30px; }\n")
                .append("        .summary { background: #e3f2fd; padding: 20px; margin: 20px 0; border-radius: 6px; border-left: 4px solid #2196f3; }\n")
                .append("        .summary h3 { margin-top: 0; color: #1976d2; }\n")
                .append("        .stats { display: flex; justify-content: space-around; margin: 20px 0; }\n")
                .append("        .stat-item { text-align: center; }\n")
                .append("        .stat-number { font-size: 2em; font-weight: bold; color: #f44336; }\n")
                .append("        .stat-label { color: #666; }\n")
                .append("        table { border-collapse: collapse; width: 100%; margin-top: 20px; }\n")
                .append("        th, td { border: 1px solid #ddd; padding: 12px 8px; text-align: left; }\n")
                .append("        th { background-color: #f2f2f2; font-weight: bold; position: sticky; top: 0; }\n")
                .append("        .missing { background-color: #ffebee; }\n")
                .append("        .extra { background-color: #e8f5e8; }\n")
                .append("        .different { background-color: #fff3e0; }\n")
                .append("        .db-name { font-weight: bold; color: #1976d2; }\n")
                .append("        .type-badge { padding: 4px 8px; border-radius: 4px; color: white; font-size: 0.85em; }\n")
                .append("        .type-missing { background-color: #f44336; }\n")
                .append("        .type-extra { background-color: #4caf50; }\n")
                .append("        .type-different { background-color: #ff9800; }\n")
                .append("        .description { max-width: 300px; word-wrap: break-word; }\n")
                .append("    </style>\n")
                .append("</head>\n")
                .append("<body>\n")
                .append("<div class='container'>\n");

        // 标题和摘要
        html.append("<h1>数据库结构差异报告</h1>\n");

        // 基准数据库信息
        if (!differences.isEmpty()) {
            SchemaDifference firstDiff = differences.get(0);
            html.append("<div class='summary'>\n")
                    .append("<h3>基准数据库信息</h3>\n")
                    .append("<p><strong>数据库：</strong><span class='db-name'>")
                    .append(escapeHtml(firstDiff.getBaseDatabaseDisplayName()))
                    .append("</span></p>\n")
                    .append("<p><strong>Schema：</strong>").append(escapeHtml(firstDiff.getSchemaName())).append("</p>\n")
                    .append("<p><strong>生成时间：</strong>")
                    .append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
                    .append("</p>\n")
                    .append("</div>\n");
        }

        // 统计信息
        if (differences.size() > 0) {
            Map<String, Long> typeStats = differences.stream()
                    .collect(java.util.stream.Collectors.groupingBy(
                            diff -> diff.getType().getDescription(),
                            java.util.stream.Collectors.counting()
                    ));

            html.append("<div class='stats'>\n")
                    .append("<div class='stat-item'>\n")
                    .append("<div class='stat-number'>").append(differences.size()).append("</div>\n")
                    .append("<div class='stat-label'>总差异数</div>\n")
                    .append("</div>\n");

            // 各类型统计
            for (Map.Entry<String, Long> entry : typeStats.entrySet()) {
                html.append("<div class='stat-item'>\n")
                        .append("<div class='stat-number'>").append(entry.getValue()).append("</div>\n")
                        .append("<div class='stat-label'>").append(entry.getKey()).append("</div>\n")
                        .append("</div>\n");
            }

            html.append("</div>\n");
        }

        // 差异表格
        html.append("<table>\n")
                .append("<thead>\n")
                .append("<tr>")
                .append("<th>基准数据库</th>")
                .append("<th>目标数据库</th>")
                .append("<th>Schema</th>")
                .append("<th>表名</th>")
                .append("<th>项目名</th>")
                .append("<th>差异类型</th>")
                .append("<th class='description'>描述</th>")
                .append("</tr>\n")
                .append("</thead>\n")
                .append("<tbody>\n");

        for (SchemaDifference diff : differences) {
            String cssClass = getCssClass(diff.getType());
            String typeBadgeClass = getTypeBadgeClass(diff.getType());

            html.append("<tr class='").append(cssClass).append("'>\n")
                    .append("<td><span class='db-name'>").append(escapeHtml(diff.getBaseDatabaseDisplayName())).append("</span></td>\n")
                    .append("<td><span class='db-name'>").append(escapeHtml(diff.getTargetDatabaseDisplayName())).append("</span></td>\n")
                    .append("<td>").append(escapeHtml(diff.getSchemaName())).append("</td>\n")
                    .append("<td>").append(escapeHtml(diff.getTableName())).append("</td>\n")
                    .append("<td>").append(escapeHtml(diff.getItemName())).append("</td>\n")
                    .append("<td><span class='type-badge ").append(typeBadgeClass).append("'>")
                    .append(diff.getType().getDescription()).append("</span></td>\n")
                    .append("<td class='description'>").append(escapeHtml(diff.getDescription())).append("</td>\n")
                    .append("</tr>\n");
        }

        html.append("</tbody>\n")
                .append("</table>\n")
                .append("</div>\n")
                .append("</body>\n")
                .append("</html>");

        try (FileWriter writer = new FileWriter(filename)) {
            writer.write(html.toString());
        }
    }

    private String getCssClass(SchemaDifference.DifferenceType type) {
        switch (type) {
            case MISSING_TABLE:
            case MISSING_COLUMN:
            case MISSING_INDEX:
                return "missing";
            case EXTRA_TABLE:
            case EXTRA_COLUMN:
            case EXTRA_INDEX:
                return "extra";
            default:
                return "different";
        }
    }

    private String getTypeBadgeClass(SchemaDifference.DifferenceType type) {
        switch (type) {
            case MISSING_TABLE:
            case MISSING_COLUMN:
            case MISSING_INDEX:
                return "type-missing";
            case EXTRA_TABLE:
            case EXTRA_COLUMN:
            case EXTRA_INDEX:
                return "type-extra";
            default:
                return "type-different";
        }
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