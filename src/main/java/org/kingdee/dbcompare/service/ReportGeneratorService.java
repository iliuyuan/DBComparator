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
     * 优化的HTML报告生成方法 - 添加差异类型过滤功能
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
        html.append("<h1>📊 数据库结构差异分析报告</h1>\n");

        if (differences.isEmpty()) {
            html.append("<div class='success-message'>\n")
                    .append("    <h2>✅ 结构完全一致</h2>\n")
                    .append("    <p>所有目标数据库的结构与基准数据库完全一致，未发现任何差异。</p>\n")
                    .append("</div>\n");
        } else {
            // 对比信息概览
            generateComparisonOverview(html, differences);

            // 添加过滤器控件
            generateFilterControls(html, differences);

            // 统计概览
            generateStatisticsOverview(html, differences);

            // 按数据库分组的差异报告
            generateGroupedDifferenceReport(html, differences);
        }

        // 添加JavaScript功能
        html.append(generateJavaScript());

        html.append("</div>\n")
                .append("</body>\n")
                .append("</html>");

        try (FileWriter writer = new FileWriter(filename)) {
            writer.write(html.toString());
        }
    }

    /**
     * 生成过滤器控件
     */
    private void generateFilterControls(StringBuilder html, List<SchemaDifference> differences) {
        // 获取所有差异类型
        Map<String, Long> typeStats = differences.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        diff -> diff.getType().toString(),
                        java.util.LinkedHashMap::new,
                        java.util.stream.Collectors.counting()
                ));

        html.append("<div class='filter-section'>\n")
                .append("    <div class='filter-header'>\n")
                .append("        <h3>🔧 过滤器</h3>\n")
                .append("        <div class='filter-actions'>\n")
                .append("            <button id='selectAll' class='filter-btn'>全选</button>\n")
                .append("            <button id='selectNone' class='filter-btn'>全不选</button>\n")
                .append("            <button id='resetFilter' class='filter-btn reset'>重置</button>\n")
                .append("        </div>\n")
                .append("    </div>\n")
                .append("    <div class='filter-controls'>\n");

        // 生成每个差异类型的复选框
        for (Map.Entry<String, Long> entry : typeStats.entrySet()) {
            String typeKey = entry.getKey();
            String typeName = SchemaDifference.DifferenceType.valueOf(typeKey).getDescription();
            Long count = entry.getValue();
            String cssClass = getFilterCssClass(typeKey);

            html.append("        <label class='filter-checkbox'>\n")
                    .append("            <input type='checkbox' value='").append(typeKey)
                    .append("' checked data-type='").append(typeKey).append("'>\n")
                    .append("            <span class='checkmark ").append(cssClass).append("'></span>\n")
                    .append("            <span class='filter-label'>").append(typeName)
                    .append(" <span class='count'>(").append(count).append(")</span></span>\n")
                    .append("        </label>\n");
        }

        html.append("    </div>\n")
                .append("    <div class='filter-stats'>\n")
                .append("        <span id='showingCount'>显示: ").append(differences.size()).append(" 项差异</span>\n")
                .append("        <span id='totalCount'>总计: ").append(differences.size()).append(" 项</span>\n")
                .append("    </div>\n")
                .append("</div>\n");
    }

    /**
     * 生成优化的CSS样式
     */
    private String generateOptimizedCSS() {
        StringBuilder css = new StringBuilder();
        css.append("* { box-sizing: border-box; margin: 0; padding: 0; }\n");
        css.append("body { font-family: 'Microsoft YaHei', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif; margin: 0; padding: 20px; background: #f8fafc; color: #2d3748; line-height: 1.6; min-height: 100vh; }\n");
        css.append(".container { max-width: 1400px; margin: 0 auto; background: white; padding: 40px; border-radius: 16px; box-shadow: 0 20px 60px rgba(0,0,0,0.15); backdrop-filter: blur(10px); }\n");
        css.append("h1 { color: #2b6cb0; text-align: center; margin-bottom: 40px; font-size: 2.8em; font-weight: 700; text-shadow: 2px 2px 4px rgba(0,0,0,0.1); position: relative; }\n");
        css.append("h1::after { content: ''; display: block; width: 100px; height: 4px; background: #667eea; margin: 15px auto 0; border-radius: 2px; }\n");

        // 过滤器样式
        css.append(".filter-section { background: #ffffff; border: 2px solid #e2e8f0; border-radius: 16px; padding: 25px; margin: 30px 0; box-shadow: 0 4px 12px rgba(0,0,0,0.05); }\n");
        css.append(".filter-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; padding-bottom: 15px; border-bottom: 2px solid #f1f5f9; }\n");
        css.append(".filter-header h3 { margin: 0; color: #2b6cb0; font-size: 1.3em; font-weight: 600; }\n");
        css.append(".filter-actions { display: flex; gap: 10px; }\n");
        css.append(".filter-btn { padding: 8px 16px; border: 2px solid #e2e8f0; background: #ffffff; color: #4a5568; border-radius: 8px; cursor: pointer; font-size: 0.9em; font-weight: 600; transition: all 0.2s ease; }\n");
        css.append(".filter-btn:hover { border-color: #667eea; color: #667eea; transform: translateY(-1px); }\n");
        css.append(".filter-btn.reset { background: #fed7d7; border-color: #fc8181; color: #c53030; }\n");
        css.append(".filter-btn.reset:hover { background: #feb2b2; border-color: #f56565; }\n");
        css.append(".filter-controls { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 15px; margin: 20px 0; }\n");
        css.append(".filter-checkbox { display: flex; align-items: center; padding: 12px 16px; background: #f8fafc; border: 2px solid #e2e8f0; border-radius: 10px; cursor: pointer; transition: all 0.2s ease; user-select: none; }\n");
        css.append(".filter-checkbox:hover { background: #edf2f7; border-color: #cbd5e0; transform: translateY(-1px); }\n");
        css.append(".filter-checkbox input { display: none; }\n");
        css.append(".checkmark { width: 20px; height: 20px; border-radius: 6px; margin-right: 12px; display: flex; align-items: center; justify-content: center; transition: all 0.2s ease; border: 2px solid #cbd5e0; background: #ffffff; }\n");
        css.append(".checkmark.critical { border-color: #e53e3e; }\n");
        css.append(".checkmark.warning { border-color: #dd6b20; }\n");
        css.append(".checkmark.info { border-color: #3182ce; }\n");
        css.append(".filter-checkbox input:checked + .checkmark { color: white; font-weight: bold; }\n");
        css.append(".filter-checkbox input:checked + .checkmark.critical { background: #e53e3e; border-color: #e53e3e; }\n");
        css.append(".filter-checkbox input:checked + .checkmark.warning { background: #dd6b20; border-color: #dd6b20; }\n");
        css.append(".filter-checkbox input:checked + .checkmark.info { background: #3182ce; border-color: #3182ce; }\n");
        css.append(".filter-checkbox input:checked + .checkmark::after { content: '✓'; font-size: 14px; font-weight: bold; }\n");
        css.append(".filter-label { flex: 1; font-weight: 600; color: #2d3748; }\n");
        css.append(".count { color: #718096; font-weight: 500; font-size: 0.9em; }\n");
        css.append(".filter-stats { display: flex; justify-content: space-between; align-items: center; padding: 15px 0 0; border-top: 2px solid #f1f5f9; margin-top: 20px; font-weight: 600; }\n");
        css.append("#showingCount { color: #2b6cb0; }\n");
        css.append("#totalCount { color: #718096; }\n");

        // 概览样式
        css.append(".comparison-overview { display: grid; grid-template-columns: 1fr 1fr; gap: 30px; margin-bottom: 40px; background: linear-gradient(135deg, #4299e1 0%, #667eea 100%); color: white; padding: 30px; border-radius: 16px; box-shadow: 0 8px 32px rgba(102, 126, 234, 0.3); }\n");
        css.append(".db-info { background: rgba(255,255,255,0.15); padding: 25px; border-radius: 12px; backdrop-filter: blur(15px); border: 1px solid rgba(255,255,255,0.2); transition: transform 0.3s ease; }\n");
        css.append(".db-info:hover { transform: translateY(-3px); }\n");
        css.append(".db-info h3 { margin: 0 0 20px 0; font-size: 1.4em; font-weight: 600; opacity: 0.95; }\n");
        css.append(".db-detail { font-size: 1.05em; margin: 10px 0; display: flex; align-items: center; }\n");
        css.append(".db-detail strong { min-width: 80px; margin-right: 10px; }\n");

        // 统计样式
        css.append(".stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 25px; margin: 40px 0; }\n");
        css.append(".stat-card { background: #ffffff; border: 2px solid #e2e8f0; border-radius: 12px; padding: 25px; text-align: center; transition: all 0.3s ease; position: relative; overflow: hidden; }\n");
        css.append(".stat-card::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 4px; background: #667eea; }\n");
        css.append(".stat-card:hover { transform: translateY(-5px); box-shadow: 0 12px 35px rgba(0,0,0,0.15); border-color: #667eea; }\n");
        css.append(".stat-number { font-size: 3em; font-weight: 800; margin-bottom: 8px; text-shadow: 1px 1px 2px rgba(0,0,0,0.1); }\n");
        css.append(".stat-number.critical { color: #e53e3e; }\n");
        css.append(".stat-number.warning { color: #dd6b20; }\n");
        css.append(".stat-number.info { color: #3182ce; }\n");
        css.append(".stat-label { color: #4a5568; font-size: 0.95em; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }\n");

        // 数据库区域样式
        css.append(".database-section { margin: 50px 0; border: 1px solid #e2e8f0; border-radius: 16px; overflow: hidden; box-shadow: 0 8px 25px rgba(0,0,0,0.08); transition: all 0.3s ease; }\n");
        css.append(".database-section:hover { box-shadow: 0 12px 40px rgba(0,0,0,0.12); }\n");
        css.append(".database-header { background: #f8fafc; padding: 25px 35px; border-bottom: 2px solid #e2e8f0; position: relative; }\n");
        css.append(".database-header::before { content: ''; position: absolute; left: 0; top: 0; bottom: 0; width: 5px; background: #667eea; }\n");
        css.append(".database-title { font-size: 1.5em; font-weight: 700; color: #1a202c; margin: 0; display: flex; align-items: center; }\n");
        css.append(".database-title::before { content: '🎯'; margin-right: 12px; font-size: 1.2em; }\n");

        // 表格样式
        css.append(".differences-table { width: 100%; border-collapse: collapse; margin: 0; background: white; table-layout: fixed; }\n");
        css.append(".differences-table th { background: #4a5568; color: white; padding: 18px 20px; text-align: left; font-weight: 700; font-size: 0.95em; text-transform: uppercase; letter-spacing: 0.5px; position: sticky; top: 0; z-index: 10; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }\n");
        css.append(".differences-table th:first-child { width: 22%; }\n");
        css.append(".differences-table th:nth-child(2) { width: 24%; }\n");
        css.append(".differences-table th:nth-child(3) { width: 14%; }\n");
        css.append(".differences-table th:nth-child(4) { width: 40%; }\n");
        css.append(".differences-table td { padding: 16px 20px; border-bottom: 1px solid #f1f5f9; vertical-align: top; transition: background-color 0.2s ease; word-wrap: break-word; overflow-wrap: break-word; }\n");
        css.append(".differences-table tr:nth-child(even) { background: #fafbfc; }\n");
        css.append(".differences-table tr:hover { background: #e2e8f0; }\n");

        // 标签样式
        css.append(".type-badge { padding: 6px 12px; border-radius: 20px; color: white; font-size: 0.8em; font-weight: 700; text-transform: uppercase; letter-spacing: 0.3px; display: inline-block; text-align: center; min-width: 90px; max-width: 100%; box-shadow: 0 2px 8px rgba(0,0,0,0.15); text-shadow: 0 1px 2px rgba(0,0,0,0.2); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }\n");
        css.append(".type-missing { background: #e53e3e; box-shadow: 0 2px 8px rgba(229, 62, 62, 0.3); }\n");
        css.append(".type-extra { background: #38a169; box-shadow: 0 2px 8px rgba(56, 161, 105, 0.3); }\n");
        css.append(".type-different { background: #dd6b20; box-shadow: 0 2px 8px rgba(221, 107, 32, 0.3); }\n");
        css.append(".table-name { font-weight: 700; color: #2b6cb0; font-size: 1.05em; display: block; word-wrap: break-word; overflow-wrap: break-word; word-break: break-all; line-height: 1.4; }\n");
        css.append(".table-name::before { content: '📋'; margin-right: 8px; font-size: 1.1em; display: inline-block; }\n");
        css.append(".item-name { font-family: 'Consolas', 'Monaco', 'Courier New', monospace; background: #f7fafc; padding: 8px 12px; border-radius: 8px; font-size: 0.9em; font-weight: 600; color: #2d3748; border: 1px solid #e2e8f0; display: inline-block; max-width: 100%; word-wrap: break-word; overflow-wrap: break-word; word-break: break-all; line-height: 1.3; }\n");
        css.append(".description { color: #4a5568; font-size: 0.95em; line-height: 1.5; word-wrap: break-word; overflow-wrap: break-word; word-break: break-word; max-width: 100%; }\n");

        // 成功消息样式
        css.append(".success-message { text-align: center; background: #38a169; color: white; padding: 50px; border-radius: 16px; margin: 40px 0; box-shadow: 0 8px 25px rgba(56, 161, 105, 0.3); }\n");
        css.append(".success-message h2 { margin: 0 0 20px 0; font-size: 2.5em; font-weight: 700; }\n");
        css.append(".success-message p { font-size: 1.2em; opacity: 0.95; }\n");
        css.append(".no-differences { text-align: center; background: #f0fff4; color: #22543d; padding: 40px; border-radius: 12px; border: 2px solid #9ae6b4; margin: 20px 0; }\n");
        css.append(".no-data-message { text-align: center; padding: 40px; color: #718096; font-style: italic; background: #f8fafc; margin: 20px; border-radius: 8px; }\n");

        // 响应式设计
        css.append("@media (max-width: 1200px) { .container { padding: 30px; } .differences-table th:first-child { width: 22%; } .differences-table th:nth-child(2) { width: 24%; } .differences-table th:nth-child(3) { width: 14%; } .differences-table th:nth-child(4) { width: 40%; } .filter-controls { grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); } }\n");
        css.append("@media (max-width: 768px) { body { padding: 10px; } .container { padding: 20px; } h1 { font-size: 2.2em; } .comparison-overview { grid-template-columns: 1fr; gap: 20px; } .stats-grid { grid-template-columns: repeat(2, 1fr); gap: 15px; } .stat-card { padding: 20px; } .stat-number { font-size: 2.2em; } .filter-header { flex-direction: column; gap: 15px; align-items: flex-start; } .filter-actions { width: 100%; justify-content: space-between; } .filter-controls { grid-template-columns: 1fr; gap: 10px; } .filter-checkbox { padding: 10px 12px; } .filter-stats { flex-direction: column; gap: 10px; align-items: flex-start; } .differences-table { font-size: 0.85em; } .differences-table th, .differences-table td { padding: 12px 10px; } .differences-table th:first-child { width: 20%; } .differences-table th:nth-child(2) { width: 26%; } .differences-table th:nth-child(3) { width: 14%; } .differences-table th:nth-child(4) { width: 40%; } .type-badge { font-size: 0.75em; padding: 6px 12px; min-width: 100px; } }\n");
        css.append("@media (max-width: 480px) { .stats-grid { grid-template-columns: 1fr; } .differences-table { font-size: 0.8em; } .database-title { font-size: 1.3em; } .filter-actions { flex-direction: column; gap: 8px; } .filter-btn { padding: 10px 16px; } }\n");

        return css.toString();
    }

    /**
     * 生成JavaScript功能
     */
    private String generateJavaScript() {
        return "<script>" +
                "document.addEventListener('DOMContentLoaded', function() {" +
                "    const checkboxes = document.querySelectorAll('.filter-checkbox input[type=\"checkbox\"]');" +
                "    const selectAllBtn = document.getElementById('selectAll');" +
                "    const selectNoneBtn = document.getElementById('selectNone');" +
                "    const resetBtn = document.getElementById('resetFilter');" +
                "    const showingCount = document.getElementById('showingCount');" +
                "" +
                "    function filterRows() {" +
                "        const checkedTypes = Array.from(checkboxes).filter(cb => cb.checked).map(cb => cb.value);" +
                "        const allRows = document.querySelectorAll('.diff-row');" +
                "        let visibleCount = 0;" +
                "" +
                "        allRows.forEach(row => {" +
                "            const rowType = row.getAttribute('data-type');" +
                "            if (checkedTypes.includes(rowType)) {" +
                "                row.style.display = '';" +
                "                visibleCount++;" +
                "            } else {" +
                "                row.style.display = 'none';" +
                "            }" +
                "        });" +
                "" +
                "        showingCount.textContent = `显示: ${visibleCount} 项差异`;" +
                "        updateStatCards(checkedTypes);" +
                "        checkEmptyDatabaseSections();" +
                "    }" +
                "" +
                "    function updateStatCards(checkedTypes) {" +
                "        const allRows = document.querySelectorAll('.diff-row');" +
                "        const typeStats = {};" +
                "        let totalVisible = 0;" +
                "" +
                "        allRows.forEach(row => {" +
                "            const rowType = row.getAttribute('data-type');" +
                "            if (checkedTypes.includes(rowType)) {" +
                "                totalVisible++;" +
                "                const typeDesc = row.querySelector('.type-badge').textContent.trim();" +
                "                typeStats[typeDesc] = (typeStats[typeDesc] || 0) + 1;" +
                "            }" +
                "        });" +
                "" +
                "        const totalCard = document.querySelector('.stat-card .stat-number.critical');" +
                "        if (totalCard) { totalCard.textContent = totalVisible; }" +
                "" +
                "        document.querySelectorAll('.stat-card').forEach(card => {" +
                "            const label = card.querySelector('.stat-label').textContent;" +
                "            if (typeStats[label] !== undefined) {" +
                "                card.querySelector('.stat-number').textContent = typeStats[label];" +
                "            } else if (label !== '总差异数') {" +
                "                card.querySelector('.stat-number').textContent = '0';" +
                "            }" +
                "        });" +
                "    }" +
                "" +
                "    function checkEmptyDatabaseSections() {" +
                "        document.querySelectorAll('.database-section').forEach(section => {" +
                "            const visibleRows = section.querySelectorAll('.diff-row[style=\"\"], .diff-row:not([style])');" +
                "            const table = section.querySelector('.differences-table');" +
                "            const noDataMsg = section.querySelector('.no-data-message');" +
                "" +
                "            if (visibleRows.length === 0 && table) {" +
                "                if (!noDataMsg) {" +
                "                    const msg = document.createElement('div');" +
                "                    msg.className = 'no-data-message';" +
                "                    msg.innerHTML = '<p style=\"text-align: center; padding: 40px; color: #718096; font-style: italic;\">当前过滤条件下无数据显示</p>';" +
                "                    table.style.display = 'none';" +
                "                    section.appendChild(msg);" +
                "                }" +
                "            } else {" +
                "                if (table) table.style.display = '';" +
                "                if (noDataMsg) noDataMsg.remove();" +
                "            }" +
                "        });" +
                "    }" +
                "" +
                "    checkboxes.forEach(checkbox => {" +
                "        checkbox.addEventListener('change', filterRows);" +
                "    });" +
                "" +
                "    selectAllBtn.addEventListener('click', function() {" +
                "        checkboxes.forEach(cb => cb.checked = true);" +
                "        filterRows();" +
                "    });" +
                "" +
                "    selectNoneBtn.addEventListener('click', function() {" +
                "        checkboxes.forEach(cb => cb.checked = false);" +
                "        filterRows();" +
                "    });" +
                "" +
                "    resetBtn.addEventListener('click', function() {" +
                "        checkboxes.forEach(cb => cb.checked = true);" +
                "        filterRows();" +
                "    });" +
                "" +
                "    filterRows();" +
                "});" +
                "</script>";
    }

    /**
     * 获取过滤器CSS类
     */
    private String getFilterCssClass(String typeKey) {
        try {
            SchemaDifference.DifferenceType type = SchemaDifference.DifferenceType.valueOf(typeKey);
            return switch (type) {
                case MISSING_TABLE, MISSING_COLUMN, MISSING_INDEX, PRIMARY_KEY_DIFF -> "critical";
                case COLUMN_DIFF, INDEX_DIFF, FOREIGN_KEY_DIFF -> "warning";
                default -> "info";
            };
        } catch (IllegalArgumentException e) {
            return "info";
        }
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
                .append("        <h3>📊 基准数据库信息</h3>\n")
                .append("        <div class='db-detail'><strong>数据库：</strong>")
                .append(escapeHtml(firstDiff.getBaseDatabaseDisplayName())).append("</div>\n")
                .append("        <div class='db-detail'><strong>Schema：</strong>")
                .append(escapeHtml(firstDiff.getSchemaName())).append("</div>\n")
                .append("    </div>\n")
                .append("    <div class='db-info'>\n")
                .append("        <h3>📈 分析统计</h3>\n")
                .append("        <div class='db-detail'><strong>生成时间：</strong>").append(generateTime).append("</div>\n")
                .append("        <div class='db-detail'><strong>总差异数：</strong>").append(differences.size()).append("</div>\n")
                .append("        <div class='db-detail'><strong>目标库数：</strong>")
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

            // 增强的数据库头部信息 - 显示"目标数据库"字样和Schema
            String schemaName = dbDiffs.isEmpty() ? "N/A" : dbDiffs.get(0).getSchemaName();
            html.append("<div class='database-header'>\n")
                    .append("    <div class='database-title'>目标数据库: ").append(escapeHtml(targetDbName))
                    .append(" <span style='color: #64748b; font-size: 0.85em; font-weight: 500;'>(Schema: ")
                    .append(escapeHtml(schemaName)).append(")</span></div>\n")
                    .append("</div>\n");

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
     * 生成差异表格 - 优化列宽比例，添加过滤属性
     */
    private void generateDifferencesTable(StringBuilder html, List<SchemaDifference> dbDiffs) {
        html.append("<table class='differences-table'>\n")
                .append("    <thead>\n")
                .append("        <tr>\n")
                .append("            <th>表名</th>\n")
                .append("            <th>项目名</th>\n")
                .append("            <th>差异类型</th>\n")
                .append("            <th>详细描述</th>\n")
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

                html.append("        <tr class='diff-row' data-type='").append(diff.getType().toString()).append("'>\n")
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

    /**
     * 获取差异类型对应的CSS类
     */
    private String getTypeBadgeClass(SchemaDifference.DifferenceType type) {
        return switch (type) {
            case MISSING_TABLE, MISSING_COLUMN, MISSING_INDEX -> "type-missing";
            case EXTRA_TABLE, EXTRA_COLUMN, EXTRA_INDEX -> "type-extra";
            default -> "type-different";
        };
    }

    /**
     * HTML转义处理
     */
    private String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }
}