package com.codeplus.digger.jdbc;

import com.codeplus.digger.core.api.Column;
import com.codeplus.digger.core.api.ReferenceColumn;
import com.codeplus.digger.core.api.Table;
import com.codeplus.digger.core.api.TablesSupplier;
import com.google.common.collect.Lists;
import io.vavr.collection.List;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Spliterator;
import java.util.Stack;
import java.util.function.Consumer;
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
    public boolean tryAdvance(Consumer<? super Table> action) {
        if(stackedTables.isEmpty()) {
            return false;
        } else {
            action.accept(stackedTables.pop());
            return true;
        }
    }

    @Override
    public Spliterator<Table> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return stackedTables.size();
    }

    @Override
    public int characteristics() {
        return SIZED;
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

            tables
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
            .dataColumns(twc.getColumnNames().map(
                c -> Column.builder().name(c).build()
            ).toList())
            .referenceColumns(referenceColumns)
            .name(twc.getTable())
            .build();
    }

    private TableWithColumns getTableWithSelectableColumns(DatabaseMetaData metaData, String t) {
        try (ResultSet rs = getColumnsResultSet(metaData, t)) {
            final List<String> columns = List.empty();
            while (rs.next()) {
                columns.append(rs.getString(COLUMN_NAME));
            }
            return new TableWithColumns(t, columns);
        } catch (Exception ex) {
            log.error("Error while loading data for table {}", t);
        }
        return new TableWithColumns(t, List.empty());
    }

    private List<ReferenceColumn> getReferenceColumns(List<String> tables, TableWithColumns twc) {
        return twc.getColumnNames()
            .filter(this::isColumnMayToPointAnotherTable)
            .filter(cn -> isColumnPointingAtTableThatExists(tables, cn))
            .map(cn -> ReferenceColumn.builder().name(cn)
                .destinationTableName(prepareTableFromColumnName(cn))
                .build())
            .toList();
    }

    private boolean isColumnPointingAtTableThatExists(List<String> tables, String columnName) {
        String correctedName = prepareTableFromColumnName(columnName);
        return tables.exists(t -> t.equalsIgnoreCase(correctedName));
    }

    private String prepareTableFromColumnName(String columnName) {
        return columnName.replace(FK, EMPTY_STRING);
    }

    private List<String> loadTables(DatabaseMetaData metaData) throws SQLException {
        final List<String> tables = List.empty();
        try (final ResultSet rsTables =
            metaData.getTables(null, null, null, new String[]{TABLE})) {
            while (rsTables.next()) {

                String tableName = rsTables.getString(TABLE_NAME);
                tables.append(tableName);
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
