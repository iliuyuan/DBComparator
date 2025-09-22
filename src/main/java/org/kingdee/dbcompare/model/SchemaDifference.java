package org.kingdee.dbcompare.model;

import lombok.Data;

// 差异记录
@Data
public class SchemaDifference {
    public enum DifferenceType {
        MISSING_TABLE("缺少表"),
        EXTRA_TABLE("多余的表"),
        MISSING_COLUMN("缺少列"),
        EXTRA_COLUMN("多余的列"),
        COLUMN_DIFF("列定义不同"),
        MISSING_INDEX("缺少索引"),
        EXTRA_INDEX("多余的索引"),
        INDEX_DIFF("索引定义不同"),
        PRIMARY_KEY_DIFF("主键不同"),
        FOREIGN_KEY_DIFF("外键不同");

        private final String description;

        DifferenceType(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    private String baseDatabaseName;        // 基准数据库名
    private String targetDatabaseName;      // 目标数据库名
    private String baseDatabaseDisplay;     // 基准数据库显示名称（包含IP）
    private String targetDatabaseDisplay;   // 目标数据库显示名称（包含IP）
    private String schemaName;              // Schema名称
    private String tableName;               // 表名
    private String itemName;                // 具体项目名（列名、索引名等）
    private DifferenceType type;            // 差异类型
    private String description;             // 详细描述
    private String baseValue;               // 基准值
    private String targetValue;             // 目标值

    public SchemaDifference(String baseDatabaseName, String targetDatabaseName,
                            String schemaName, String tableName, String itemName,
                            DifferenceType type, String description) {
        this.baseDatabaseName = baseDatabaseName;
        this.targetDatabaseName = targetDatabaseName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.itemName = itemName;
        this.type = type;
        this.description = description;
    }

    // 新增带显示名称的构造函数
    public SchemaDifference(String baseDatabaseName, String targetDatabaseName,
                            String baseDatabaseDisplay, String targetDatabaseDisplay,
                            String schemaName, String tableName, String itemName,
                            DifferenceType type, String description) {
        this(baseDatabaseName, targetDatabaseName, schemaName, tableName, itemName, type, description);
        this.baseDatabaseDisplay = baseDatabaseDisplay;
        this.targetDatabaseDisplay = targetDatabaseDisplay;
    }

    public SchemaDifference(String baseDatabaseName, String targetDatabaseName,
                            String schemaName, String tableName, String itemName,
                            DifferenceType type, String description,
                            String baseValue, String targetValue) {
        this(baseDatabaseName, targetDatabaseName, schemaName, tableName, itemName, type, description);
        this.baseValue = baseValue;
        this.targetValue = targetValue;
    }

    // 新增带显示名称和值的构造函数
    public SchemaDifference(String baseDatabaseName, String targetDatabaseName,
                            String baseDatabaseDisplay, String targetDatabaseDisplay,
                            String schemaName, String tableName, String itemName,
                            DifferenceType type, String description,
                            String baseValue, String targetValue) {
        this(baseDatabaseName, targetDatabaseName, baseDatabaseDisplay, targetDatabaseDisplay,
                schemaName, tableName, itemName, type, description);
        this.baseValue = baseValue;
        this.targetValue = targetValue;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("[%s] ", type.getDescription()));

        if (schemaName != null && !schemaName.equals("public")) {
            sb.append(String.format("%s.%s.%s", schemaName, tableName, itemName));
        } else {
            sb.append(String.format("%s.%s", tableName, itemName));
        }

        // 优先使用显示名称（包含IP），如果没有则使用原名称
        String baseDisplayName = baseDatabaseDisplay != null ? baseDatabaseDisplay : baseDatabaseName;
        String targetDisplayName = targetDatabaseDisplay != null ? targetDatabaseDisplay : targetDatabaseName;

        sb.append(String.format(" - 基准库: %s, 目标库: %s", baseDisplayName, targetDisplayName));

        if (description != null && !description.isEmpty()) {
            sb.append(" - ").append(description);
        }

        if (baseValue != null && targetValue != null) {
            sb.append(String.format(" (基准: %s -> 目标: %s)", baseValue, targetValue));
        }

        return sb.toString();
    }

    /**
     * 获取基准数据库的显示名称（包含IP）
     */
    public String getBaseDatabaseDisplayName() {
        return baseDatabaseDisplay != null ? baseDatabaseDisplay : baseDatabaseName;
    }

    /**
     * 获取目标数据库的显示名称（包含IP）
     */
    public String getTargetDatabaseDisplayName() {
        return targetDatabaseDisplay != null ? targetDatabaseDisplay : targetDatabaseName;
    }

    // 保持向后兼容的方法
    @Deprecated
    public String getDatabaseName() {
        return targetDatabaseName;
    }
}