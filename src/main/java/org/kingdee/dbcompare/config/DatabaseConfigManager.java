package org.kingdee.dbcompare.config;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Component
@ConfigurationProperties(prefix = "app")
@Data
public class DatabaseConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseConfigManager.class);

    /**
     * 从配置文件加载数据库配置
     */
    public static DatabaseConfigSet loadDatabaseConfig(String configFile) throws IOException {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(configFile)) {
            props.load(fis);
        }

        // 加载基准数据库
        DatabaseComparatorConfig baseDb = new DatabaseComparatorConfig(
                props.getProperty("base.database.name"),
                props.getProperty("base.database.url"),
                props.getProperty("base.database.username"),
                props.getProperty("base.database.password"),
                props.getProperty("base.database.schema", "public") // 从配置读取schema，默认为public
        );

        logger.info("基准数据库配置: 名称={}, Schema={}", baseDb.getName(), baseDb.getSchema());

        // 加载目标数据库
        List<DatabaseComparatorConfig> targetDbs = new ArrayList<>();
        int targetCount = Integer.parseInt(props.getProperty("target.database.count", "1"));

        for (int i = 1; i <= targetCount; i++) {
            String name = props.getProperty("target.database." + i + ".name");
            String url = props.getProperty("target.database." + i + ".url");
            String username = props.getProperty("target.database." + i + ".username");
            String password = props.getProperty("target.database." + i + ".password");
            String schema = props.getProperty("target.database." + i + ".schema", "public"); // 从配置读取schema

            if (name != null && url != null && username != null && password != null) {
                DatabaseComparatorConfig targetDb = new DatabaseComparatorConfig(name, url, username, password, schema);
                targetDbs.add(targetDb);
                logger.info("目标数据库{}配置: 名称={}, Schema={}", i, targetDb.getName(), targetDb.getSchema());
            }
        }

        return new DatabaseConfigSet(baseDb, targetDbs);
    }

    @Data
    public static class DatabaseConfigSet {
        private final DatabaseComparatorConfig baseDatabase;
        private final List<DatabaseComparatorConfig> targetDatabases;

        public DatabaseConfigSet(DatabaseComparatorConfig baseDatabase, List<DatabaseComparatorConfig> targetDatabases) {
            this.baseDatabase = baseDatabase;
            this.targetDatabases = targetDatabases;
        }
    }
}