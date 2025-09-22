package org.kingdee.dbcompare.model;

import lombok.Data;

import java.util.List;
import java.util.Objects;

// 索引信息
@Data
public class IndexInfo {
    private String indexName;
    private boolean unique;
    private List<String> columns;
    private String indexType;

    public IndexInfo(String indexName, boolean unique, List<String> columns, String indexType) {
        this.indexName = indexName;
        this.unique = unique;
        this.columns = columns;
        this.indexType = indexType;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        IndexInfo indexInfo = (IndexInfo) obj;
        return unique == indexInfo.unique &&
                Objects.equals(columns, indexInfo.columns) &&
                Objects.equals(indexType, indexInfo.indexType);
    }
}

