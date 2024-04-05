package com.dbdataplacement.dbdataplacement.utils;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class TableProperties {

    private static final Map<String, TableConfig> tables = new HashMap<>();

    public static  Map<String, TableConfig> getTables()  {
        return tables;
    }

    static {
        try {
            // Load the YAML file
            InputStream inputStream = TableProperties.class.getClassLoader().getResourceAsStream("application.yml");
            Yaml yaml = new Yaml();

            // Parse the YAML and extract tables
            Map<String, Map<String, Object>> yamlData = yaml.load(inputStream);
            Map<String, Object> tablesMap = yamlData.get("tables");

            // Iterate through the tables and populate TableProperties
            for (Map.Entry<String, Object> entry : tablesMap.entrySet()) {
                String tableName = entry.getKey();
                Map<String, Object> tableConfig = (Map<String, Object>) entry.getValue();

                TableConfig innerConfig = new TableConfig();
                innerConfig.setName((String) tableConfig.get("name"));
                innerConfig.setArchiveTableName((String) tableConfig.get("archiveTableName"));
                innerConfig.setCondition((String) tableConfig.get("condition"));

                tables.put(tableName, innerConfig);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading YAML file: " + e.getMessage(), e);
        }
    }

    public static class TableConfig {
        private String name;
        private String archiveTableName;
        private String condition;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getArchiveTableName() {
            return archiveTableName;
        }

        public void setArchiveTableName(String archiveTableName) {
            this.archiveTableName = archiveTableName;
        }

        public String getCondition() {
            return condition;
        }

        public void setCondition(String condition) {
            this.condition = condition;
        }
    }
}
