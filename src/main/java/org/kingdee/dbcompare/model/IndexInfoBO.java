package org.kingdee.dbcompare.model;

import lombok.Data;

import java.util.List;
import java.util.Objects;

// 索引信息
@Data
public class IndexInfoBO {
    private String indexName;
    private boolean unique;
    private List<String> columns;
    private String indexType;

    public IndexInfoBO(String indexName, boolean unique, List<String> columns, String indexType) {
        this.indexName = indexName;
        this.unique = unique;
        this.columns = columns;
        this.indexType = indexType;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        IndexInfoBO indexInfoBO = (IndexInfoBO) obj;
        return unique == indexInfoBO.unique &&
                Objects.equals(columns, indexInfoBO.columns) &&
                Objects.equals(indexType, indexInfoBO.indexType);
    }
}

