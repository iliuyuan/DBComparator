package org.kingdee.dbcompare.config;

/**
 * 配置类：报告生成策略配置
 */
class ReportConfig {

    // 多层级报告阈值配置
    public static final int SMALL_SCALE_THRESHOLD = 5;      // 小规模：≤5个数据库
    public static final int MEDIUM_SCALE_THRESHOLD = 20;     // 中等规模：6-20个数据库
    public static final int LARGE_SCALE_THRESHOLD = 50;      // 大规模：21-50个数据库
    // 超大规模：>50个数据库

    // 文件大小控制
    public static final int MAX_SINGLE_FILE_DIFFERENCES = 1000;  // 单文件最大差异数
    public static final int MAX_HTML_TABLE_ROWS = 500;           // HTML表格最大行数

    // 批处理配置
    public static final int BATCH_PROCESS_SIZE = 10;             // 批处理大小

    /**
     * 根据数据库数量获取推荐策略
     */
    public static ReportStrategy getRecommendedStrategy(int databaseCount) {
        if (databaseCount <= SMALL_SCALE_THRESHOLD) {
            return ReportStrategy.SINGLE_FILE;
        } else if (databaseCount <= MEDIUM_SCALE_THRESHOLD) {
            return ReportStrategy.MULTI_LEVEL_BASIC;
        } else if (databaseCount <= LARGE_SCALE_THRESHOLD) {
            return ReportStrategy.MULTI_LEVEL_ENHANCED;
        } else {
            return ReportStrategy.DISTRIBUTED;
        }
    }

    public enum ReportStrategy {
        SINGLE_FILE,           // 单文件报告
        MULTI_LEVEL_BASIC,     // 基础多层级
        MULTI_LEVEL_ENHANCED,  // 增强多层级
        DISTRIBUTED           // 分布式报告（超大规模）
    }
}
