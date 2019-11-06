package com.codeplus.digger.jdbc;

import com.codeplus.digger.core.api.Column;
import com.codeplus.digger.core.api.ReferenceColumn;
import com.codeplus.digger.core.api.Table;
import com.codeplus.digger.core.api.TablesSupplier;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcTablesSupplier implements TablesSupplier {

    private static final String TABLE = "TABLE";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String FK = "FK";
    private static final String EMPTY_STRING = "";
    private final Stack<Table> stackedTables;

    private DataSource dataSource;

    public JdbcTablesSupplier(DataSource dataSource) {
        this.dataSource = dataSource;
        this.stackedTables = init();
    }

    @Override
    public Table get() {
        return stackedTables.pop();
    }

    @Data
    @AllArgsConstructor
    private static final class TableWithColumns {

        private String table;
        private List<String> columnNames;

    }

    private Stack<Table> init() {

        Stack<Table> stack = new Stack<>();

        try (Connection connection = dataSource.getConnection()) {

            final DatabaseMetaData metaData = connection.getMetaData();

            final List<String> tables = loadTables(metaData);

            log.info("Loaded {} tables", tables.size());

            tables.stream()
                .map(t -> getTableWithSelectableColumns(metaData, t))
                .map(twc -> mapToTable(tables, twc))
                .forEach(
                    t -> stack.push(t)
                );

            return stack;

        } catch (Exception ex) {
            log.error("Digging for data failed", ex);
            return stack;
        }
    }

    private Table mapToTable(List<String> tables, TableWithColumns twc) {
        final List<ReferenceColumn> referenceColumns = getReferenceColumns(tables,
            twc);

        return Table.builder()
            .dataColumns(twc.getColumnNames().stream().map(
                c -> Column.builder().name(c).build()
            ).collect(Collectors.toList()))
            .referenceColumns(referenceColumns)
            .name(twc.getTable())
            .build();
    }

    private TableWithColumns getTableWithSelectableColumns(DatabaseMetaData metaData, String t) {
        try (ResultSet rs = getColumnsResultSet(metaData, t)) {
            final ArrayList<String> columns = new ArrayList<>();
            while (rs.next()) {
                columns.add(rs.getString(COLUMN_NAME));
            }
            return new TableWithColumns(t, columns);
        } catch (Exception ex) {
            log.error("Error while loading data for table {}", t);
        }
        return new TableWithColumns(t, new ArrayList<>());
    }

    private List<ReferenceColumn> getReferenceColumns(List<String> tables, TableWithColumns twc) {
        return twc.getColumnNames().stream()
            .filter(this::isColumnMayToPointAnotherTable)
            .filter(cn -> isColumnPointingAtTableThatExists(tables, cn))
            .map(cn -> ReferenceColumn.builder().name(cn)
                .destinationTableName(prepareTableFromColumnName(cn))
                .build())
            .collect(Collectors.toList());
    }

    private boolean isColumnPointingAtTableThatExists(List<String> tables, String columnName) {
        String correctedName = prepareTableFromColumnName(columnName);
        return tables.stream()
            .anyMatch(t -> t.equalsIgnoreCase(correctedName));
    }

    private String prepareTableFromColumnName(String columnName) {
        return columnName.replace(FK, EMPTY_STRING);
    }

    private List<String> loadTables(DatabaseMetaData metaData) throws SQLException {
        final List<String> tables = Lists.newArrayList();
        try (final ResultSet rsTables =
            metaData.getTables(null, null, null, new String[]{TABLE})) {
            while (rsTables.next()) {

                String tableName = rsTables.getString(TABLE_NAME);
                tables.add(tableName);
            }

        }
        return tables;
    }

    private boolean isColumnMayToPointAnotherTable(String columnNameFK) {
        return columnNameFK.endsWith(FK);
    }

    private ResultSet getColumnsResultSet(DatabaseMetaData metaData, String table)
        throws SQLException {
        return metaData.getColumns(null, null, table, null);
    }
}
