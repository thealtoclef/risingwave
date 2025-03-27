package com.risingwave.connector.source;

import static com.risingwave.connector.source.ValidatorUtils.invalidArgument;

import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.risingwave.proto.TableSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SpannerValidator extends DatabaseValidator implements AutoCloseable {
    private final Map<String, String> props;
    private final TableSchema tableSchema;
    private final boolean isCdcSourceJob;
    private final Spanner spanner;
    private final DatabaseClient dbClient;

    private static final String REQUIRED_PROPS = "project_id,instance_id,database_id,change_stream_name";

    public SpannerValidator(Map<String, String> props, TableSchema tableSchema, boolean isCdcSourceJob) {
        this.props = props;
        this.tableSchema = tableSchema;
        this.isCdcSourceJob = isCdcSourceJob;
        
        String projectId = props.get("project_id");
        String instanceId = props.get("instance_id");
        String databaseId = props.get("database_id");
        
        // Create Spanner client
        SpannerOptions options = SpannerOptions.newBuilder().setProjectId(projectId).build();
        this.spanner = options.getService();
        this.dbClient = spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
    }

    @Override
    public void validateAll() throws Exception {
        validateDbConfig();
        validateChangeStream();
        validateTable();
        validateTableSchema();
    }

    private void validateDbConfig() throws Exception {
        try {
            // Verify connection works
            dbClient.singleUse().executeQuery(Statement.of("SELECT 1"));
        } catch (Exception e) {
            throw invalidArgument(
                    String.format("Failed to connect to Spanner: %s", e.getMessage()));
        }
    }

    private void validateChangeStream() throws Exception {
        String changeStreamName = props.get("change_stream_name");
        
        try {
            ResultSet resultSet = dbClient.singleUse().executeQuery(
                Statement.of("SELECT COUNT(*) FROM INFORMATION_SCHEMA.CHANGE_STREAMS " +
                           "WHERE CHANGE_STREAM_NAME = '" + changeStreamName + "'")
            );
            
            if (resultSet.next() && resultSet.getLong(0) == 0) {
                throw invalidArgument(
                    String.format("Change stream '%s' does not exist", changeStreamName));
            }
        } catch (Exception e) {
            throw invalidArgument(
                String.format("Error validating change stream: %s", e.getMessage()));
        }
    }

    private void validateTable() throws Exception {
        if (!isCdcSourceJob) {
            String tableName = props.get("table");
            String schema = props.getOrDefault("schema.name", "");
            
            String fullTableName = schema.isEmpty() ? tableName : schema + "." + tableName;
            
            try {
                // Check if table exists
                ResultSet resultSet = dbClient.singleUse().executeQuery(
                    Statement.of("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES " +
                               "WHERE TABLE_NAME = '" + tableName + "'" +
                               (schema.isEmpty() ? "" : " AND TABLE_SCHEMA = '" + schema + "'"))
                );
                
                if (resultSet.next() && resultSet.getLong(0) == 0) {
                    throw invalidArgument(
                        String.format("Table '%s' does not exist", fullTableName));
                }
                
                // Check if table is captured by change stream
                String changeStreamName = props.get("change_stream_name");
                resultSet = dbClient.singleUse().executeQuery(
                    Statement.of("SELECT COUNT(*) FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES " +
                               "WHERE CHANGE_STREAM_NAME = '" + changeStreamName + "' " +
                               "AND TABLE_NAME = '" + tableName + "'" +
                               (schema.isEmpty() ? "" : " AND TABLE_SCHEMA = '" + schema + "'"))
                );
                
                if (resultSet.next() && resultSet.getLong(0) == 0) {
                    throw invalidArgument(
                        String.format("Table '%s' is not captured by change stream '%s'", 
                                    fullTableName, changeStreamName));
                }
                
                // Validate primary key
                validatePrimaryKey(fullTableName);
            } catch (Exception e) {
                throw invalidArgument(
                    String.format("Error validating table: %s", e.getMessage()));
            }
        }
    }

    private void validatePrimaryKey(String fullTableName) throws Exception {
        // Get primary key columns from Spanner
        ResultSet resultSet = dbClient.singleUse().executeQuery(
            Statement.of("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                       "WHERE TABLE_NAME = '" + fullTableName + "' " +
                       "ORDER BY ORDINAL_POSITION")
        );
        
        List<String> pkColumns = new ArrayList<>();
        while (resultSet.next()) {
            pkColumns.add(resultSet.getString("COLUMN_NAME").toLowerCase());
        }
        
        if (pkColumns.isEmpty()) {
            throw invalidArgument(
                String.format("Table '%s' has no primary key", fullTableName));
        }
        
        // Compare with the expected primary key columns from the schema
        List<String> expectedPkColumns = new ArrayList<>();
        for (int idx : tableSchema.getPkIndicesList()) {
            expectedPkColumns.add(tableSchema.getColumns(idx).getName().toLowerCase());
        }
        
        if (!pkColumns.equals(expectedPkColumns)) {
            throw invalidArgument(
                String.format("Primary key mismatch. Spanner table has %s, but schema expects %s",
                            pkColumns, expectedPkColumns));
        }
    }

    private void validateTableSchema() throws Exception {
        if (!isCdcSourceJob) {
            String tableName = props.get("table");
            String schema = props.getOrDefault("schema.name", "");
            
            // Get column information from Spanner
            ResultSet resultSet = dbClient.singleUse().executeQuery(
                Statement.of("SELECT COLUMN_NAME, SPANNER_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
                           "WHERE TABLE_NAME = '" + tableName + "'" +
                           (schema.isEmpty() ? "" : " AND TABLE_SCHEMA = '" + schema + "'"))
            );
            
            Map<String, String> columnTypes = new HashMap<>();
            while (resultSet.next()) {
                columnTypes.put(
                    resultSet.getString("COLUMN_NAME").toLowerCase(),
                    resultSet.getString("SPANNER_TYPE")
                );
            }
            
            // Compare with expected schema
            for (com.risingwave.proto.plan_common.ColumnDesc col : tableSchema.getColumnsList()) {
                String colName = col.getName().toLowerCase();
                if (!columnTypes.containsKey(colName)) {
                    throw invalidArgument(
                        String.format("Column '%s' not found in Spanner table", colName));
                }
                
                if (!isDataTypeCompatible(col.getTypeDesc().getTypeName(), columnTypes.get(colName))) {
                    throw invalidArgument(
                        String.format("Data type mismatch for column '%s': Spanner has %s, schema expects %s",
                                   colName, columnTypes.get(colName), col.getTypeDesc().getTypeName()));
                }
            }
        }
    }

    private boolean isDataTypeCompatible(String schemaType, String spannerType) {
        // Map RisingWave types to Spanner types
        spannerType = spannerType.toUpperCase();
        
        switch (schemaType.toUpperCase()) {
            case "BOOLEAN":
                return spannerType.contains("BOOL");
            case "SMALLINT":
            case "INT":
            case "BIGINT":
                return spannerType.contains("INT64");
            case "REAL":
            case "DOUBLE":
                return spannerType.contains("FLOAT64");
            case "VARCHAR":
            case "CHAR":
            case "TEXT":
                return spannerType.contains("STRING");
            case "DATE":
                return spannerType.contains("DATE");
            case "TIMESTAMP":
                return spannerType.contains("TIMESTAMP");
            case "BYTEA":
                return spannerType.contains("BYTES");
            default:
                return false;
        }
    }

    @Override
    public void close() {
        if (spanner != null) {
            spanner.close();
        }
    }
}
