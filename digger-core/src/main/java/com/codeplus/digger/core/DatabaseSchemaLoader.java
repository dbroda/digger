package com.codeplus.digger.core;

import com.google.common.collect.Lists;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class DatabaseSchemaLoader {

    private DataSource dataSource;

    public DatabaseSchemaLoader(DataSource dataSource) {

        this.dataSource = dataSource;
    }

    public List<Table> loadSchema() throws SQLException {

        final DatabaseMetaData metaData = dataSource.getConnection().getMetaData();
        final List<Table> tables = loadTables(metaData);

        log.info("Loaded {} tables", tables.size());

        tables.parallelStream().forEach(
            table -> {
                try (ResultSet columns = metaData.getColumns(null, null, table.getName(), null);
                ) {
                    while (columns.next()) {
                        String columnNameFK = columns.getString("COLUMN_NAME");
                        log.debug("Analyzing column {} for table {}", columnNameFK,
                            table.getName());

                        if (columnNameFK.endsWith("FK")) {
                            String columnName = columnNameFK.replace("FK", "");
                            if (tables.stream()
                                .anyMatch(t -> t.getName().equalsIgnoreCase(columnName))) {
                                table.getReferenceColumns().add(Column.builder().name(columnName).build());
                                log.info("Column {}.{} matched to table", table.getName(),
                                    columnName);
                            }
                        }

                        table.getDataColumns().add(Column.builder().name(columnNameFK).build());
                    }

                } catch (SQLException e) {
                    log.error("Error while processing table {}", table, e);
                }
            }
        );

        return tables;
    }

    private List<Table> loadTables(DatabaseMetaData metaData) throws SQLException {
        final List<Table> tables = Lists.newArrayList();
        try (ResultSet rsTables = metaData.getTables(null, null, null, new String[]{"TABLE"})) {
            while (rsTables.next()) {

                String tableName = rsTables.getString("TABLE_NAME");
                Table table = Table.builder().name(tableName).build();
                tables.add(table);
            }

        }
        return tables;
    }
}
