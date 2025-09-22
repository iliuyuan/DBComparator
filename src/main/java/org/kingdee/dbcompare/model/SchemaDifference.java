package org.kingdee.dbcompare.model;

import lombok.Data;

// 差异记录
@Data
public class SchemaDifference {
    public enum DifferenceType {
        MISSING_TABLE,      // 缺少表
        EXTRA_TABLE,        // 多余的表
        MISSING_COLUMN,     // 缺少列
        EXTRA_COLUMN,       // 多余的列
        COLUMN_DIFF,        // 列定义不同
        MISSING_INDEX,      // 缺少索引
        EXTRA_INDEX,        // 多余的索引
        INDEX_DIFF,         // 索引定义不同
        PRIMARY_KEY_DIFF    // 主键不同
    }

    private String databaseName;
    private String tableName;
    private String itemName;
    private DifferenceType type;
    private String description;

    public SchemaDifference(String databaseName, String tableName, String itemName,
                            DifferenceType type, String description) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.itemName = itemName;
        this.type = type;
        this.description = description;
    }

    // getters
    public String getDatabaseName() { return databaseName; }
    public String getTableName() { return tableName; }
    public String getItemName() { return itemName; }
    public DifferenceType getType() { return type; }
    public String getDescription() { return description; }

    @Override
    public String toString() {
        return String.format("[%s] %s.%s - %s: %s",
                type, tableName, itemName, databaseName, description);
    }
}
