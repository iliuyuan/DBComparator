package org.kingdee.dbcompare.service;

import org.kingdee.dbcompare.config.DatabaseComparatorConfig;
import org.kingdee.dbcompare.model.ColumnInfoBO;
import org.kingdee.dbcompare.model.IndexInfoBO;
import org.kingdee.dbcompare.model.SchemaDifference;
import org.kingdee.dbcompare.model.TableInfoBO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PostgreSQL数据库结构对比工具
 * 以某个基准数据库的结构为标准，对比其他数据库的差异
 */
public class PgSQLDBComparator {

    private static final Logger logger = LoggerFactory.getLogger(PgSQLDBComparator.class);

    private DatabaseComparatorConfig baseDatabase;
    private List<DatabaseComparatorConfig> targetDatabases;
    private Map<String, TableInfoBO> baseSchema;
    private List<SchemaDifference> differences;

    public PgSQLDBComparator(DatabaseComparatorConfig baseDatabase, List<DatabaseComparatorConfig> targetDatabases) {
        this.baseDatabase = baseDatabase;
        this.targetDatabases = targetDatabases;
        this.differences = new ArrayList<>();
    }

    /**
     * 执行数据库结构对比
     */
    public void compareSchemas() throws SQLException {
        logger.info("开始加载基准数据库结构: {} (Schema: {})", baseDatabase.getDisplayName(), baseDatabase.getSchema());
        baseSchema = loadDatabaseSchema(baseDatabase);
        logger.info("基准数据库加载完成，共 {} 个表", baseSchema.size());

        differences.clear();

        for (DatabaseComparatorConfig targetDb : targetDatabases) {
            logger.info("正在对比数据库: {} (Schema: {})", targetDb.getDisplayName(), targetDb.getSchema());
            Map<String, TableInfoBO> targetSchema = loadDatabaseSchema(targetDb);
            compareTwoSchemas(baseSchema, targetSchema, targetDb);
        }

        logger.info("对比完成，共发现 {} 个差异", differences.size());
    }

    /**
     * 加载数据库结构信息
     */
    private Map<String, TableInfoBO> loadDatabaseSchema(DatabaseComparatorConfig dbConfig) throws SQLException {
        Map<String, TableInfoBO> schema = new ConcurrentHashMap<>();

        try (Connection conn = DriverManager.getConnection(
                dbConfig.getUrl(), dbConfig.getUsername(), dbConfig.getPassword())) {

            logger.debug("连接数据库成功: {}, Schema: {}", dbConfig.getDisplayName(), dbConfig.getSchema());

            // 加载表信息
            loadTables(conn, schema, dbConfig.getSchema());

            // 加载列信息
            loadColumns(conn, schema, dbConfig.getSchema());

            // 加载主键信息
            loadPrimaryKeys(conn, schema, dbConfig.getSchema());

            // 加载索引信息
            loadIndexes(conn, schema, dbConfig.getSchema());
        }

        return schema;
    }

    /**
     * 加载表信息
     */
    private void loadTables(Connection conn, Map<String, TableInfoBO> schema, String schemaName) throws SQLException {
        String sql = "SELECT table_name, table_type " +
                "FROM information_schema.tables " +
                "WHERE table_schema = ? " +
                "ORDER BY table_name";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, schemaName);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    String tableType = rs.getString("table_type");
                    schema.put(tableName, new TableInfoBO(tableName, tableType));
                }
            }
        }

        logger.debug("从Schema '{}' 加载了 {} 个表", schemaName, schema.size());
    }

    /**
     * 加载列信息
     */
    private void loadColumns(Connection conn, Map<String, TableInfoBO> schema, String schemaName) throws SQLException {
        String sql = "SELECT table_name, column_name, data_type, is_nullable, " +
                "column_default, character_maximum_length, numeric_precision, numeric_scale " +
                "FROM information_schema.columns " +
                "WHERE table_schema = ? " +
                "ORDER BY table_name, ordinal_position";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, schemaName);

            try (ResultSet rs = stmt.executeQuery()) {
                int columnCount = 0;
                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    TableInfoBO tableInfoBO = schema.get(tableName);
                    if (tableInfoBO != null) {
                        String columnName = rs.getString("column_name");
                        String dataType = rs.getString("data_type");
                        boolean nullable = "YES".equals(rs.getString("is_nullable"));
                        String defaultValue = rs.getString("column_default");
                        int charMaxLength = rs.getInt("character_maximum_length");
                        int numericPrecision = rs.getInt("numeric_precision");
                        int numericScale = rs.getInt("numeric_scale");

                        ColumnInfoBO columnInfoBO = new ColumnInfoBO(columnName, dataType, nullable,
                                defaultValue, charMaxLength, numericPrecision, numericScale);

                        tableInfoBO.getColumns().put(columnName, columnInfoBO);
                        columnCount++;
                    }
                }
                logger.debug("从Schema '{}' 加载了 {} 个列", schemaName, columnCount);
            }
        }
    }

    /**
     * 加载主键信息
     */
    private void loadPrimaryKeys(Connection conn, Map<String, TableInfoBO> schema, String schemaName) throws SQLException {
        String sql = "SELECT tc.table_name, kcu.column_name " +
                "FROM information_schema.table_constraints tc " +
                "JOIN information_schema.key_column_usage kcu " +
                "ON tc.constraint_name = kcu.constraint_name " +
                "WHERE tc.constraint_type = 'PRIMARY KEY' " +
                "AND tc.table_schema = ? " +
                "ORDER BY tc.table_name, kcu.ordinal_position";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, schemaName);

            try (ResultSet rs = stmt.executeQuery()) {
                int pkCount = 0;
                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    String columnName = rs.getString("column_name");

                    TableInfoBO tableInfoBO = schema.get(tableName);
                    if (tableInfoBO != null) {
                        tableInfoBO.getPrimaryKeys().add(columnName);
                        pkCount++;
                    }
                }
                logger.debug("从Schema '{}' 加载了 {} 个主键列", schemaName, pkCount);
            }
        }
    }

    /**
     * 加载索引信息
     */
    private void loadIndexes(Connection conn, Map<String, TableInfoBO> schema, String schemaName) throws SQLException {
        String sql = "SELECT i.schemaname, i.tablename, i.indexname, " +
                "i.indexdef, ix.indisunique " +
                "FROM pg_indexes i " +
                "JOIN pg_class c ON c.relname = i.indexname " +
                "JOIN pg_index ix ON ix.indexrelid = c.oid " +
                "WHERE i.schemaname = ? " +
                "AND i.indexname NOT LIKE '%_pkey' " +
                "ORDER BY i.tablename, i.indexname";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, schemaName);

            try (ResultSet rs = stmt.executeQuery()) {
                int indexCount = 0;
                while (rs.next()) {
                    String tableName = rs.getString("tablename");
                    String indexName = rs.getString("indexname");
                    String indexDef = rs.getString("indexdef");
                    boolean isUnique = rs.getBoolean("indisunique");

                    TableInfoBO tableInfoBO = schema.get(tableName);
                    if (tableInfoBO != null) {
                        // 解析索引定义获取列信息
                        List<String> indexColumns = parseIndexColumns(indexDef);
                        String indexType = parseIndexType(indexDef);

                        IndexInfoBO indexInfoBO = new IndexInfoBO(indexName, isUnique, indexColumns, indexType);
                        tableInfoBO.getIndexes().put(indexName, indexInfoBO);
                        indexCount++;
                    }
                }
                logger.debug("从Schema '{}' 加载了 {} 个索引", schemaName, indexCount);
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
     * 对比两个数据库结构（优化版本，使用DatabaseConfig而不是字符串）
     */
    private void compareTwoSchemas(Map<String, TableInfoBO> baseSchema,
                                   Map<String, TableInfoBO> targetSchema,
                                   DatabaseComparatorConfig targetDb) {

        logger.info("正在对比: {} vs {}", baseDatabase.getDisplayName(), targetDb.getDisplayName());

        // 检查缺少的表和多余的表
        for (String tableName : baseSchema.keySet()) {
            if (!targetSchema.containsKey(tableName)) {
                differences.add(new SchemaDifference(
                        baseDatabase.getName(), targetDb.getName(),
                        baseDatabase.getDisplayName(), targetDb.getDisplayName(),
                        baseDatabase.getSchema(), tableName, tableName,
                        SchemaDifference.DifferenceType.MISSING_TABLE,
                        String.format("目标数据库 '%s' 缺少表 '%s'", targetDb.getDisplayName(), tableName)
                ));
                continue;
            }

            // 对比表结构
            compareTableStructure(baseSchema.get(tableName),
                    targetSchema.get(tableName), targetDb);
        }

        // 检查多余的表
        for (String tableName : targetSchema.keySet()) {
            if (!baseSchema.containsKey(tableName)) {
                differences.add(new SchemaDifference(
                        baseDatabase.getName(), targetDb.getName(),
                        baseDatabase.getDisplayName(), targetDb.getDisplayName(),
                        baseDatabase.getSchema(), tableName, tableName,
                        SchemaDifference.DifferenceType.EXTRA_TABLE,
                        String.format("目标数据库 '%s' 多出表 '%s'", targetDb.getDisplayName(), tableName)
                ));
            }
        }
    }

    /**
     * 对比表结构（优化版本）
     */
    private void compareTableStructure(TableInfoBO baseTable, TableInfoBO targetTable, DatabaseComparatorConfig targetDb) {
        // 对比列
        compareColumns(baseTable, targetTable, targetDb);

        // 对比主键
        comparePrimaryKeys(baseTable, targetTable, targetDb);

        // 对比索引
        compareIndexes(baseTable, targetTable, targetDb);
    }

    /**
     * 对比列（优化版本）
     */
    private void compareColumns(TableInfoBO baseTable, TableInfoBO targetTable, DatabaseComparatorConfig targetDb) {
        String tableName = baseTable.getTableName();
        Map<String, ColumnInfoBO> baseColumns = baseTable.getColumns();
        Map<String, ColumnInfoBO> targetColumns = targetTable.getColumns();

        // 检查缺少的列和不同的列
        for (String columnName : baseColumns.keySet()) {
            if (!targetColumns.containsKey(columnName)) {
                differences.add(new SchemaDifference(
                        baseDatabase.getName(), targetDb.getName(),
                        baseDatabase.getDisplayName(), targetDb.getDisplayName(),
                        baseDatabase.getSchema(), tableName, columnName,
                        SchemaDifference.DifferenceType.MISSING_COLUMN,
                        String.format("目标数据库 '%s' 的表 '%s' 缺少列 '%s'", targetDb.getDisplayName(), tableName, columnName)
                ));
            } else {
                ColumnInfoBO baseColumn = baseColumns.get(columnName);
                ColumnInfoBO targetColumn = targetColumns.get(columnName);

                if (!baseColumn.equals(targetColumn)) {
                    String desc = buildColumnDifferenceDescription(baseColumn, targetColumn);
                    differences.add(new SchemaDifference(
                            baseDatabase.getName(), targetDb.getName(),
                            baseDatabase.getDisplayName(), targetDb.getDisplayName(),
                            baseDatabase.getSchema(), tableName, columnName,
                            SchemaDifference.DifferenceType.COLUMN_DIFF, desc
                    ));
                }
            }
        }

        // 检查多余的列
        for (String columnName : targetColumns.keySet()) {
            if (!baseColumns.containsKey(columnName)) {
                differences.add(new SchemaDifference(
                        baseDatabase.getName(), targetDb.getName(),
                        baseDatabase.getDisplayName(), targetDb.getDisplayName(),
                        baseDatabase.getSchema(), tableName, columnName,
                        SchemaDifference.DifferenceType.EXTRA_COLUMN,
                        String.format("目标数据库 '%s' 的表 '%s' 多出列 '%s'", targetDb.getDisplayName(), tableName, columnName)
                ));
            }
        }
    }

    /**
     * 构建列差异描述（增强版）
     */
    private String buildColumnDifferenceDescription(ColumnInfoBO base, ColumnInfoBO target) {
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
     * 对比主键（优化版本）
     */
    private void comparePrimaryKeys(TableInfoBO baseTable, TableInfoBO targetTable, DatabaseComparatorConfig targetDb) {
        String tableName = baseTable.getTableName();
        Set<String> basePrimaryKeys = baseTable.getPrimaryKeys();
        Set<String> targetPrimaryKeys = targetTable.getPrimaryKeys();

        if (!basePrimaryKeys.equals(targetPrimaryKeys)) {
            String baseKeysStr = basePrimaryKeys.isEmpty() ? "无" : String.join(", ", basePrimaryKeys);
            String targetKeysStr = targetPrimaryKeys.isEmpty() ? "无" : String.join(", ", targetPrimaryKeys);

            String desc = String.format("主键定义不同 - 基准库: [%s], 目标库: [%s]", baseKeysStr, targetKeysStr);

            differences.add(new SchemaDifference(
                    baseDatabase.getName(), targetDb.getName(),
                    baseDatabase.getDisplayName(), targetDb.getDisplayName(),
                    baseDatabase.getSchema(), tableName, "PRIMARY_KEY",
                    SchemaDifference.DifferenceType.PRIMARY_KEY_DIFF, desc,
                    baseKeysStr, targetKeysStr
            ));
        }
    }

    /**
     * 对比索引（优化版本）
     */
    private void compareIndexes(TableInfoBO baseTable, TableInfoBO targetTable, DatabaseComparatorConfig targetDb) {
        String tableName = baseTable.getTableName();
        Map<String, IndexInfoBO> baseIndexes = baseTable.getIndexes();
        Map<String, IndexInfoBO> targetIndexes = targetTable.getIndexes();

        // 检查缺少的索引和不同的索引
        for (String indexName : baseIndexes.keySet()) {
            if (!targetIndexes.containsKey(indexName)) {
                IndexInfoBO baseIndex = baseIndexes.get(indexName);
                String desc = String.format("目标数据库 '%s' 的表 '%s' 缺少索引 '%s' (列: %s, 类型: %s, 唯一: %s)",
                        targetDb.getDisplayName(), tableName, indexName,
                        String.join(", ", baseIndex.getColumns()),
                        baseIndex.getIndexType(),
                        baseIndex.isUnique() ? "是" : "否");

                differences.add(new SchemaDifference(
                        baseDatabase.getName(), targetDb.getName(),
                        baseDatabase.getDisplayName(), targetDb.getDisplayName(),
                        baseDatabase.getSchema(), tableName, indexName,
                        SchemaDifference.DifferenceType.MISSING_INDEX, desc
                ));
            } else {
                IndexInfoBO baseIndex = baseIndexes.get(indexName);
                IndexInfoBO targetIndex = targetIndexes.get(indexName);

                if (!baseIndex.equals(targetIndex)) {
                    String desc = buildIndexDifferenceDescription(baseIndex, targetIndex);
                    differences.add(new SchemaDifference(
                            baseDatabase.getName(), targetDb.getName(),
                            baseDatabase.getDisplayName(), targetDb.getDisplayName(),
                            baseDatabase.getSchema(), tableName, indexName,
                            SchemaDifference.DifferenceType.INDEX_DIFF, desc
                    ));
                }
            }
        }

        /**
         * 对比索引（优化版本）- 续
         */
        // 检查多余的索引
        for (String indexName : targetIndexes.keySet()) {
            if (!baseIndexes.containsKey(indexName)) {
                IndexInfoBO targetIndex = targetIndexes.get(indexName);
                String desc = String.format("目标数据库 '%s' 的表 '%s' 多出索引 '%s' (列: %s, 类型: %s, 唯一: %s)",
                        targetDb.getDisplayName(), tableName, indexName,
                        String.join(", ", targetIndex.getColumns()),
                        targetIndex.getIndexType(),
                        targetIndex.isUnique() ? "是" : "否");

                differences.add(new SchemaDifference(
                        baseDatabase.getName(), targetDb.getName(),
                        baseDatabase.getDisplayName(), targetDb.getDisplayName(),
                        baseDatabase.getSchema(), tableName, indexName,
                        SchemaDifference.DifferenceType.EXTRA_INDEX, desc
                ));
            }
        }
    }

    /**
     * 构建索引差异描述（增强版）
     */
    private String buildIndexDifferenceDescription(IndexInfoBO base, IndexInfoBO target) {
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
     * 按数据库获取差异（使用显示名称）
     */
    public Map<String, List<SchemaDifference>> getDifferencesByDatabase() {
        Map<String, List<SchemaDifference>> result = new HashMap<>();
        for (SchemaDifference diff : differences) {
            String key = diff.getTargetDatabaseDisplayName();
            result.computeIfAbsent(key, k -> new ArrayList<>()).add(diff);
        }
        return result;
    }

    /**
     * 打印增强的差异报告（优化显示IP信息）
     */
    public void printDifferencesReport() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("               数据库结构差异详细报告");
        System.out.println("=".repeat(80));
        System.out.println("基准数据库: " + baseDatabase.getDisplayName() + " (Schema: " + baseDatabase.getSchema() + ")");
        System.out.println("对比时间: " + java.time.LocalDateTime.now().format(
                java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        System.out.println("总差异数: " + differences.size());

        if (differences.isEmpty()) {
            System.out.println("\n✓ 所有数据库结构完全一致，未发现任何差异！");
            return;
        }

        // 按数据库分组显示（使用显示名称）
        Map<String, List<SchemaDifference>> byDatabase = getDifferencesByDatabase();

        for (String dbDisplayName : byDatabase.keySet()) {
            System.out.println("\n" + "-".repeat(60));
            System.out.println(String.format("目标数据库: %s", dbDisplayName));
            System.out.println("-".repeat(60));

            List<SchemaDifference> dbDiffs = byDatabase.get(dbDisplayName);

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
     * 导出差异到CSV文件（优化为显示IP信息）
     */
    public void exportDifferencesToCSV(String filename) throws java.io.IOException {
        try (java.io.PrintWriter writer = new java.io.PrintWriter(filename, "UTF-8")) {
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