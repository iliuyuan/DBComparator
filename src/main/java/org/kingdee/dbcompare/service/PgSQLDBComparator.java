package org.kingdee.dbcompare.service;

import org.kingdee.dbcompare.config.DatabaseConfig;
import org.kingdee.dbcompare.model.ColumnInfo;
import org.kingdee.dbcompare.model.IndexInfo;
import org.kingdee.dbcompare.model.SchemaDifference;
import org.kingdee.dbcompare.model.TableInfo;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PostgreSQL数据库结构对比工具
 * 以某个基准数据库的结构为标准，对比其他数据库的差异
 */
public class PgSQLDBComparator {

    private DatabaseConfig baseDatabase;
    private List<DatabaseConfig> targetDatabases;
    private Map<String, TableInfo> baseSchema;
    private List<SchemaDifference> differences;

    public PgSQLDBComparator(DatabaseConfig baseDatabase, List<DatabaseConfig> targetDatabases) {
        this.baseDatabase = baseDatabase;
        this.targetDatabases = targetDatabases;
        this.differences = new ArrayList<>();
    }

    /**
     * 执行数据库结构对比
     */
    public void compareSchemas() throws SQLException {
        System.out.println("开始加载基准数据库结构: " + baseDatabase.getName());
        baseSchema = loadDatabaseSchema(baseDatabase);
        System.out.println("基准数据库加载完成，共 " + baseSchema.size() + " 个表");

        differences.clear();

        for (DatabaseConfig targetDb : targetDatabases) {
            System.out.println("正在对比数据库: " + targetDb.getName());
            Map<String, TableInfo> targetSchema = loadDatabaseSchema(targetDb);
            compareTwoSchemas(baseSchema, targetSchema, targetDb.getName());
        }

        System.out.println("对比完成，共发现 " + differences.size() + " 个差异");
    }

    /**
     * 加载数据库结构信息
     */
    private Map<String, TableInfo> loadDatabaseSchema(DatabaseConfig dbConfig) throws SQLException {
        Map<String, TableInfo> schema = new ConcurrentHashMap<>();

        try (Connection conn = DriverManager.getConnection(
                dbConfig.getUrl(), dbConfig.getUsername(), dbConfig.getPassword())) {

            // 加载表信息
            loadTables(conn, schema);

            // 加载列信息
            loadColumns(conn, schema);

            // 加载主键信息
            loadPrimaryKeys(conn, schema);

            // 加载索引信息
            loadIndexes(conn, schema);
        }

        return schema;
    }

    /**
     * 加载表信息
     */
    private void loadTables(Connection conn, Map<String, TableInfo> schema) throws SQLException {
        String sql = "SELECT table_name, table_type " +
                "FROM information_schema.tables " +
                "WHERE table_schema = 'public' " +
                "ORDER BY table_name";

        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                String tableName = rs.getString("table_name");
                String tableType = rs.getString("table_type");
                schema.put(tableName, new TableInfo(tableName, tableType));
            }
        }
    }

    /**
     * 加载列信息
     */
    private void loadColumns(Connection conn, Map<String, TableInfo> schema) throws SQLException {
        String sql = "SELECT table_name, column_name, data_type, is_nullable, " +
                "column_default, character_maximum_length, numeric_precision, numeric_scale " +
                "FROM information_schema.columns " +
                "WHERE table_schema = 'public' " +
                "ORDER BY table_name, ordinal_position";

        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                String tableName = rs.getString("table_name");
                TableInfo tableInfo = schema.get(tableName);
                if (tableInfo != null) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    boolean nullable = "YES".equals(rs.getString("is_nullable"));
                    String defaultValue = rs.getString("column_default");
                    int charMaxLength = rs.getInt("character_maximum_length");
                    int numericPrecision = rs.getInt("numeric_precision");
                    int numericScale = rs.getInt("numeric_scale");

                    ColumnInfo columnInfo = new ColumnInfo(columnName, dataType, nullable,
                            defaultValue, charMaxLength, numericPrecision, numericScale);

                    tableInfo.getColumns().put(columnName, columnInfo);
                }
            }
        }
    }

    /**
     * 加载主键信息
     */
    private void loadPrimaryKeys(Connection conn, Map<String, TableInfo> schema) throws SQLException {
        String sql = "SELECT tc.table_name, kcu.column_name " +
                "FROM information_schema.table_constraints tc " +
                "JOIN information_schema.key_column_usage kcu " +
                "ON tc.constraint_name = kcu.constraint_name " +
                "WHERE tc.constraint_type = 'PRIMARY KEY' " +
                "AND tc.table_schema = 'public' " +
                "ORDER BY tc.table_name, kcu.ordinal_position";

        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                String tableName = rs.getString("table_name");
                String columnName = rs.getString("column_name");

                TableInfo tableInfo = schema.get(tableName);
                if (tableInfo != null) {
                    tableInfo.getPrimaryKeys().add(columnName);
                }
            }
        }
    }

    /**
     * 加载索引信息
     */
    private void loadIndexes(Connection conn, Map<String, TableInfo> schema) throws SQLException {
        String sql = "SELECT i.schemaname, i.tablename, i.indexname, " +
                "i.indexdef, ix.indisunique " +
                "FROM pg_indexes i " +
                "JOIN pg_class c ON c.relname = i.indexname " +
                "JOIN pg_index ix ON ix.indexrelid = c.oid " +
                "WHERE i.schemaname = 'public' " +
                "AND i.indexname NOT LIKE '%_pkey' " +
                "ORDER BY i.tablename, i.indexname";

        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                String tableName = rs.getString("tablename");
                String indexName = rs.getString("indexname");
                String indexDef = rs.getString("indexdef");
                boolean isUnique = rs.getBoolean("indisunique");

                TableInfo tableInfo = schema.get(tableName);
                if (tableInfo != null) {
                    // 解析索引定义获取列信息
                    List<String> indexColumns = parseIndexColumns(indexDef);
                    String indexType = parseIndexType(indexDef);

                    IndexInfo indexInfo = new IndexInfo(indexName, isUnique, indexColumns, indexType);
                    tableInfo.getIndexes().put(indexName, indexInfo);
                }
            }
        }
    }

    /**
     * 解析索引列
     */
    private List<String> parseIndexColumns(String indexDef) {
        List<String> columns = new ArrayList<>();
        int start = indexDef.indexOf("(");
        int end = indexDef.lastIndexOf(")");
        if (start > 0 && end > start) {
            String columnsPart = indexDef.substring(start + 1, end);
            for (String col : columnsPart.split(",")) {
                columns.add(col.trim());
            }
        }
        return columns;
    }

    /**
     * 解析索引类型
     */
    private String parseIndexType(String indexDef) {
        if (indexDef.toLowerCase().contains(" using btree")) {
            return "btree";
        } else if (indexDef.toLowerCase().contains(" using hash")) {
            return "hash";
        } else if (indexDef.toLowerCase().contains(" using gin")) {
            return "gin";
        } else if (indexDef.toLowerCase().contains(" using gist")) {
            return "gist";
        }
        return "btree"; // 默认类型
    }

    /**
     * 对比两个数据库结构
     */
    private void compareTwoSchemas(Map<String, TableInfo> baseSchema,
                                   Map<String, TableInfo> targetSchema,
                                   String targetDbName) {

        System.out.println(String.format("正在对比: %s vs %s", baseDatabase.getName(), targetDbName));

        // 检查缺少的表和多余的表
        for (String tableName : baseSchema.keySet()) {
            if (!targetSchema.containsKey(tableName)) {
                differences.add(new SchemaDifference(
                        baseDatabase.getName(), targetDbName, "public", tableName, tableName,
                        SchemaDifference.DifferenceType.MISSING_TABLE,
                        String.format("目标数据库 '%s' 缺少表 '%s'", targetDbName, tableName)
                ));
                continue;
            }

            // 对比表结构
            compareTableStructure(baseSchema.get(tableName),
                    targetSchema.get(tableName), targetDbName);
        }

        // 检查多余的表
        for (String tableName : targetSchema.keySet()) {
            if (!baseSchema.containsKey(tableName)) {
                differences.add(new SchemaDifference(
                        baseDatabase.getName(), targetDbName, "public", tableName, tableName,
                        SchemaDifference.DifferenceType.EXTRA_TABLE,
                        String.format("目标数据库 '%s' 多出表 '%s'", targetDbName, tableName)
                ));
            }
        }
    }

    /**
     * 对比表结构
     */
    private void compareTableStructure(TableInfo baseTable, TableInfo targetTable, String targetDbName) {
        // 对比列
        compareColumns(baseTable, targetTable, targetDbName);

        // 对比主键
        comparePrimaryKeys(baseTable, targetTable, targetDbName);

        // 对比索引
        compareIndexes(baseTable, targetTable, targetDbName);
    }

    /**
     * 对比列
     */
    private void compareColumns(TableInfo baseTable, TableInfo targetTable, String targetDbName) {
        String tableName = baseTable.getTableName();
        Map<String, ColumnInfo> baseColumns = baseTable.getColumns();
        Map<String, ColumnInfo> targetColumns = targetTable.getColumns();

        // 检查缺少的列和不同的列
        for (String columnName : baseColumns.keySet()) {
            if (!targetColumns.containsKey(columnName)) {
                differences.add(new SchemaDifference(
                        baseDatabase.getName(), targetDbName, "public", tableName, columnName,
                        SchemaDifference.DifferenceType.MISSING_COLUMN,
                        String.format("目标数据库 '%s' 的表 '%s' 缺少列 '%s'", targetDbName, tableName, columnName)
                ));
            } else {
                ColumnInfo baseColumn = baseColumns.get(columnName);
                ColumnInfo targetColumn = targetColumns.get(columnName);

                if (!baseColumn.equals(targetColumn)) {
                    String desc = buildColumnDifferenceDescription(baseColumn, targetColumn);
                    differences.add(new SchemaDifference(
                            baseDatabase.getName(), targetDbName, "public", tableName, columnName,
                            SchemaDifference.DifferenceType.COLUMN_DIFF, desc
                    ));
                }
            }
        }

        // 检查多余的列
        for (String columnName : targetColumns.keySet()) {
            if (!baseColumns.containsKey(columnName)) {
                differences.add(new SchemaDifference(
                        baseDatabase.getName(), targetDbName, "public", tableName, columnName,
                        SchemaDifference.DifferenceType.EXTRA_COLUMN,
                        String.format("目标数据库 '%s' 的表 '%s' 多出列 '%s'", targetDbName, tableName, columnName)
                ));
            }
        }
    }


    /**
     * 构建列差异描述（增强版）
     */
    private String buildColumnDifferenceDescription(ColumnInfo base, ColumnInfo target) {
        List<String> diffs = new ArrayList<>();

        if (!Objects.equals(base.getDataType(), target.getDataType())) {
            diffs.add(String.format("数据类型不同 - 基准: %s, 目标: %s", base.getDataType(), target.getDataType()));
        }

        if (base.isNullable() != target.isNullable()) {
            diffs.add(String.format("可空性不同 - 基准: %s, 目标: %s",
                    base.isNullable() ? "可空" : "不可空",
                    target.isNullable() ? "可空" : "不可空"));
        }

        if (!Objects.equals(base.getDefaultValue(), target.getDefaultValue())) {
            String baseDefault = base.getDefaultValue() == null ? "无" : base.getDefaultValue();
            String targetDefault = target.getDefaultValue() == null ? "无" : target.getDefaultValue();
            diffs.add(String.format("默认值不同 - 基准: %s, 目标: %s", baseDefault, targetDefault));
        }

        if (base.getCharacterMaxLength() != target.getCharacterMaxLength() &&
                base.getCharacterMaxLength() > 0 && target.getCharacterMaxLength() > 0) {
            diffs.add(String.format("最大长度不同 - 基准: %d, 目标: %d",
                    base.getCharacterMaxLength(), target.getCharacterMaxLength()));
        }

        if (base.getNumericPrecision() != target.getNumericPrecision() &&
                base.getNumericPrecision() > 0 && target.getNumericPrecision() > 0) {
            diffs.add(String.format("数值精度不同 - 基准: %d, 目标: %d",
                    base.getNumericPrecision(), target.getNumericPrecision()));
        }

        if (base.getNumericScale() != target.getNumericScale() &&
                base.getNumericScale() > 0 && target.getNumericScale() > 0) {
            diffs.add(String.format("数值标度不同 - 基准: %d, 目标: %d",
                    base.getNumericScale(), target.getNumericScale()));
        }

        return String.join("; ", diffs);
    }

    /**
     * 对比主键
     */
    private void comparePrimaryKeys(TableInfo baseTable, TableInfo targetTable, String targetDbName) {
        String tableName = baseTable.getTableName();
        Set<String> basePrimaryKeys = baseTable.getPrimaryKeys();
        Set<String> targetPrimaryKeys = targetTable.getPrimaryKeys();

        if (!basePrimaryKeys.equals(targetPrimaryKeys)) {
            String baseKeysStr = basePrimaryKeys.isEmpty() ? "无" : String.join(", ", basePrimaryKeys);
            String targetKeysStr = targetPrimaryKeys.isEmpty() ? "无" : String.join(", ", targetPrimaryKeys);

            String desc = String.format("主键定义不同 - 基准库: [%s], 目标库: [%s]", baseKeysStr, targetKeysStr);

            differences.add(new SchemaDifference(
                    baseDatabase.getName(), targetDbName, "public", tableName, "PRIMARY_KEY",
                    SchemaDifference.DifferenceType.PRIMARY_KEY_DIFF, desc,
                    baseKeysStr, targetKeysStr
            ));
        }
    }

    /**
     * 对比索引
     */
    private void compareIndexes(TableInfo baseTable, TableInfo targetTable, String targetDbName) {
        String tableName = baseTable.getTableName();
        Map<String, IndexInfo> baseIndexes = baseTable.getIndexes();
        Map<String, IndexInfo> targetIndexes = targetTable.getIndexes();

        // 检查缺少的索引和不同的索引
        for (String indexName : baseIndexes.keySet()) {
            if (!targetIndexes.containsKey(indexName)) {
                IndexInfo baseIndex = baseIndexes.get(indexName);
                String desc = String.format("目标数据库 '%s' 的表 '%s' 缺少索引 '%s' (列: %s, 类型: %s, 唯一: %s)",
                        targetDbName, tableName, indexName,
                        String.join(", ", baseIndex.getColumns()),
                        baseIndex.getIndexType(),
                        baseIndex.isUnique() ? "是" : "否");

                differences.add(new SchemaDifference(
                        baseDatabase.getName(), targetDbName, "public", tableName, indexName,
                        SchemaDifference.DifferenceType.MISSING_INDEX, desc
                ));
            } else {
                IndexInfo baseIndex = baseIndexes.get(indexName);
                IndexInfo targetIndex = targetIndexes.get(indexName);

                if (!baseIndex.equals(targetIndex)) {
                    String desc = buildIndexDifferenceDescription(baseIndex, targetIndex);
                    differences.add(new SchemaDifference(
                            baseDatabase.getName(), targetDbName, "public", tableName, indexName,
                            SchemaDifference.DifferenceType.INDEX_DIFF, desc
                    ));
                }
            }
        }

        // 检查多余的索引
        for (String indexName : targetIndexes.keySet()) {
            if (!baseIndexes.containsKey(indexName)) {
                IndexInfo targetIndex = targetIndexes.get(indexName);
                String desc = String.format("目标数据库 '%s' 的表 '%s' 多出索引 '%s' (列: %s, 类型: %s, 唯一: %s)",
                        targetDbName, tableName, indexName,
                        String.join(", ", targetIndex.getColumns()),
                        targetIndex.getIndexType(),
                        targetIndex.isUnique() ? "是" : "否");

                differences.add(new SchemaDifference(
                        baseDatabase.getName(), targetDbName, "public", tableName, indexName,
                        SchemaDifference.DifferenceType.EXTRA_INDEX, desc
                ));
            }
        }
    }

    /**
     * 构建索引差异描述（增强版）
     */
    private String buildIndexDifferenceDescription(IndexInfo base, IndexInfo target) {
        List<String> diffs = new ArrayList<>();

        if (base.isUnique() != target.isUnique()) {
            diffs.add(String.format("唯一性不同 - 基准: %s, 目标: %s",
                    base.isUnique() ? "唯一" : "非唯一",
                    target.isUnique() ? "唯一" : "非唯一"));
        }

        if (!Objects.equals(base.getColumns(), target.getColumns())) {
            diffs.add(String.format("索引列不同 - 基准: [%s], 目标: [%s]",
                    String.join(", ", base.getColumns()),
                    String.join(", ", target.getColumns())));
        }

        if (!Objects.equals(base.getIndexType(), target.getIndexType())) {
            diffs.add(String.format("索引类型不同 - 基准: %s, 目标: %s",
                    base.getIndexType(), target.getIndexType()));
        }

        return String.join("; ", diffs);
    }

    /**
     * 获取所有差异
     */
    public List<SchemaDifference> getDifferences() {
        return new ArrayList<>(differences);
    }

    /**
     * 按类型获取差异
     */
    public List<SchemaDifference> getDifferencesByType(SchemaDifference.DifferenceType type) {
        return differences.stream()
                .filter(diff -> diff.getType() == type)
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }

    /**
     * 按数据库获取差异（使用新的字段）
     */
    public Map<String, List<SchemaDifference>> getDifferencesByDatabase() {
        Map<String, List<SchemaDifference>> result = new HashMap<>();
        for (SchemaDifference diff : differences) {
            result.computeIfAbsent(diff.getTargetDatabaseName(), k -> new ArrayList<>()).add(diff);
        }
        return result;
    }

    /**
     * 打印增强的差异报告
     */
    public void printDifferencesReport() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("               数据库结构差异详细报告");
        System.out.println("=".repeat(80));
        System.out.println("基准数据库: " + baseDatabase.getName());
        System.out.println("对比时间: " + java.time.LocalDateTime.now().format(
                java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        System.out.println("总差异数: " + differences.size());

        if (differences.isEmpty()) {
            System.out.println("\n✓ 所有数据库结构完全一致，未发现任何差异！");
            return;
        }

        // 按数据库分组显示
        Map<String, List<SchemaDifference>> byDatabase = getDifferencesByDatabase();

        for (String dbName : byDatabase.keySet()) {
            System.out.println("\n" + "-".repeat(60));
            System.out.println(String.format("目标数据库: %s", dbName));
            System.out.println("-".repeat(60));

            List<SchemaDifference> dbDiffs = byDatabase.get(dbName);

            // 统计各类型差异数量
            Map<SchemaDifference.DifferenceType, Long> typeCount = dbDiffs.stream()
                    .collect(java.util.stream.Collectors.groupingBy(
                            SchemaDifference::getType,
                            java.util.LinkedHashMap::new,  // 保持顺序
                            java.util.stream.Collectors.counting()));

            System.out.println("差异统计:");
            typeCount.forEach((type, count) ->
                    System.out.println(String.format("  %-15s: %3d 个", type.getDescription(), count)));

            System.out.println("\n详细差异:");

            // 按类型分组显示详细差异
            Map<SchemaDifference.DifferenceType, List<SchemaDifference>> byType = dbDiffs.stream()
                    .collect(java.util.stream.Collectors.groupingBy(
                            SchemaDifference::getType,
                            java.util.LinkedHashMap::new,
                            java.util.stream.Collectors.toList()));

            byType.forEach((type, diffs) -> {
                System.out.println(String.format("\n  【%s】", type.getDescription()));
                diffs.forEach(diff -> System.out.println("    " + formatDifferenceForConsole(diff)));
            });
        }

        System.out.println("\n" + "=".repeat(80));
    }

    /**
     * 格式化差异信息用于控制台显示
     */
    private String formatDifferenceForConsole(SchemaDifference diff) {
        StringBuilder sb = new StringBuilder();

        if (diff.getSchemaName() != null && !diff.getSchemaName().equals("public")) {
            sb.append(String.format("%s.%s.%s", diff.getSchemaName(), diff.getTableName(), diff.getItemName()));
        } else {
            sb.append(String.format("%s.%s", diff.getTableName(), diff.getItemName()));
        }

        if (diff.getDescription() != null && !diff.getDescription().isEmpty()) {
            sb.append(" - ").append(diff.getDescription());
        }

        return sb.toString();
    }

    /**
     * 导出差异到CSV文件
     */
    public void exportDifferencesToCSV(String filename) throws java.io.IOException {
        try (java.io.PrintWriter writer = new java.io.PrintWriter(filename, "UTF-8")) {
            writer.println("基准数据库,目标数据库,Schema,表名,项目名,差异类型,详细描述");

            for (SchemaDifference diff : differences) {
                writer.printf("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"%n",
                        escapeCsvValue(diff.getBaseDatabaseName()),
                        escapeCsvValue(diff.getTargetDatabaseName()),
                        escapeCsvValue(diff.getSchemaName() != null ? diff.getSchemaName() : "public"),
                        escapeCsvValue(diff.getTableName()),
                        escapeCsvValue(diff.getItemName()),
                        escapeCsvValue(diff.getType().getDescription()),
                        escapeCsvValue(diff.getDescription()));
            }
        }
        System.out.println("差异报告已导出到: " + filename);
    }

    /**
     * CSV值转义处理
     */
    private String escapeCsvValue(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\"", "\"\"");
    }
}
