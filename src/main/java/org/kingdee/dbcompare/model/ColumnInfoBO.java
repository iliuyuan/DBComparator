package org.kingdee.dbcompare.model;

import lombok.Data;

import java.util.Objects;

// 列信息
@Data
public class ColumnInfoBO {
    private String columnName;
    private String dataType;
    private boolean nullable;
    private String defaultValue;
    private int characterMaxLength;
    private int numericPrecision;
    private int numericScale;

    public ColumnInfoBO(String columnName, String dataType, boolean nullable,
                        String defaultValue, int characterMaxLength,
                        int numericPrecision, int numericScale) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.nullable = nullable;
        this.defaultValue = defaultValue;
        this.characterMaxLength = characterMaxLength;
        this.numericPrecision = numericPrecision;
        this.numericScale = numericScale;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ColumnInfoBO that = (ColumnInfoBO) obj;
        return nullable == that.nullable &&
                characterMaxLength == that.characterMaxLength &&
                numericPrecision == that.numericPrecision &&
                numericScale == that.numericScale &&
                Objects.equals(columnName, that.columnName) &&
                Objects.equals(dataType, that.dataType) &&
                Objects.equals(defaultValue, that.defaultValue);
    }
}
