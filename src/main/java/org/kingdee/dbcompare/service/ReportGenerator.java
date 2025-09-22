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
     * 导出到CSV格式
     */
    public void exportToCSV(List<SchemaDifference> differences, String filename) throws IOException {
        try (FileWriter writer = new FileWriter(filename);
             CSVPrinter csvPrinter = new CSVPrinter(writer,
                     CSVFormat.DEFAULT.withHeader("基准数据库", "目标数据库", "Schema", "表名", "项目名", "差异类型", "描述", "时间戳"))) {

            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            for (SchemaDifference diff : differences) {
                csvPrinter.printRecord(
                        diff.getBaseDatabaseName(),
                        diff.getTargetDatabaseName(),
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
     * 导出到JSON格式
     */
    public void exportToJSON(List<SchemaDifference> differences, String filename) throws IOException {
        Map<String, Object> report = new HashMap<>();
        report.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        report.put("totalDifferences", differences.size());
        report.put("differences", differences);

        // 按类型统计
        Map<String, Long> typeStats = differences.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        diff -> diff.getType().toString(),
                        java.util.stream.Collectors.counting()
                ));
        report.put("statisticsByType", typeStats);

        // 按数据库统计
        Map<String, Long> dbStats = differences.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        SchemaDifference::getDatabaseName,
                        java.util.stream.Collectors.counting()
                ));
        report.put("statisticsByDatabase", dbStats);

        objectMapper.writerWithDefaultPrettyPrinter().writeValue(new java.io.File(filename), report);
    }

    /**
     * 生成HTML报告
     */
    public void exportToHTML(List<SchemaDifference> differences, String filename) throws IOException {
        StringBuilder html = new StringBuilder();

        html.append("<!DOCTYPE html>\n")
                .append("<html lang='zh-CN'>\n")
                .append("<head>\n")
                .append("    <meta charset='UTF-8'>\n")
                .append("    <title>数据库结构差异报告</title>\n")
                .append("    <style>\n")
                .append("        body { font-family: Arial, sans-serif; margin: 40px; }\n")
                .append("        h1 { color: #333; }\n")
                .append("        .summary { background: #f5f5f5; padding: 15px; margin: 20px 0; }\n")
                .append("        table { border-collapse: collapse; width: 100%; }\n")
                .append("        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }\n")
                .append("        th { background-color: #f2f2f2; }\n")
                .append("        .missing { background-color: #ffebee; }\n")
                .append("        .extra { background-color: #e8f5e8; }\n")
                .append("        .different { background-color: #fff3e0; }\n")
                .append("    </style>\n")
                .append("</head>\n")
                .append("<body>\n");

        // 标题和摘要
        html.append("<h1>数据库结构差异报告</h1>\n")
                .append("<div class='summary'>\n")
                .append("<p><strong>生成时间：</strong>")
                .append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
                .append("</p>\n")
                .append("<p><strong>总差异数：</strong>").append(differences.size()).append("</p>\n")
                .append("</div>\n");

        // 差异表格
        html.append("<table>\n")
                .append("<thead>\n")
                .append("<tr><th>基准数据库</th><th>目标数据库</th><th>Schema</th><th>表名</th><th>项目名</th><th>差异类型</th><th>描述</th></tr>\n")
                .append("</thead>\n")
                .append("<tbody>\n");

        for (SchemaDifference diff : differences) {
            String cssClass = getCssClass(diff.getType());
            html.append("<tr class='").append(cssClass).append("'>\n")
                    .append("<td>").append(escapeHtml(diff.getBaseDatabaseName())).append("</td>\n")
                    .append("<td>").append(escapeHtml(diff.getTargetDatabaseName())).append("</td>\n")
                    .append("<td>").append(escapeHtml(diff.getSchemaName())).append("</td>\n")
                    .append("<td>").append(escapeHtml(diff.getTableName())).append("</td>\n")
                    .append("<td>").append(escapeHtml(diff.getItemName())).append("</td>\n")
                    .append("<td>").append(diff.getType().getDescription()).append("</td>\n")
                    .append("<td>").append(escapeHtml(diff.getDescription())).append("</td>\n")
                    .append("</tr>\n");
        }

        html.append("</tbody>\n")
                .append("</table>\n")
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

    private String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }
}