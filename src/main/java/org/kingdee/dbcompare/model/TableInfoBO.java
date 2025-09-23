package org.kingdee.dbcompare.model;

import lombok.Data;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// 表信息
@Data
public class TableInfoBO {
    private String tableName;
    private String tableType;
    private Map<String, ColumnInfoBO> columns = new HashMap<>();
    private Map<String, IndexInfoBO> indexes = new HashMap<>();
    private Set<String> primaryKeys = new HashSet<>();

    public TableInfoBO(String tableName, String tableType) {
        this.tableName = tableName;
        this.tableType = tableType;
    }
}
