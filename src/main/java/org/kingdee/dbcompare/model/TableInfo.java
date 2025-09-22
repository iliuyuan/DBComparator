package org.kingdee.dbcompare.model;

import lombok.Data;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// 表信息
@Data
public class TableInfo {
    private String tableName;
    private String tableType;
    private Map<String, ColumnInfo> columns = new HashMap<>();
    private Map<String, IndexInfo> indexes = new HashMap<>();
    private Set<String> primaryKeys = new HashSet<>();

    public TableInfo(String tableName, String tableType) {
        this.tableName = tableName;
        this.tableType = tableType;
    }
}
