package com.codeplus.digger.core;

import com.codeplus.digger.core.api.CommandsToScript;
import com.codeplus.digger.core.api.ID;
import com.codeplus.digger.core.api.ReferenceColumn;
import com.codeplus.digger.core.api.Script;
import com.codeplus.digger.core.api.TablesSupplier;
import com.google.common.graph.MutableGraph;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Set;
import io.vavr.gson.VavrGson;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Spliterator;
import java.util.Stack;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public final class Digger<T extends ID> {

    public static final boolean NO_PARALLEL = false;


    public static Digger instance(CommandsToScript commandsToScript) {
        return new Digger(
            commandsToScript,
            new GraphFromTablesBuilder(),
            new GraphToCommandsService()
        );
    }


    private CommandsToScript commandsToScripts;
    private GraphFromTablesBuilder graphBuilder;
    private GraphToCommandsService graphToCommandsService;


    public void createScript(String startingNode, TablesSupplier tableSupplier, List<T> ids) {

        try {
//            List<com.codeplus.digger.core.api.Table> apiTables = StreamSupport
//                .stream(tableSupplier, false)
//                .collect(Collectors.toList());

//            saveToFile(apiTables);

            FileTablesSupplier fileTablesSupplier = new FileTablesSupplier();

            final List<Table> tables = mapApiToInternalTables(fileTablesSupplier);
            //prepare graph
            MutableGraph<Table> graph = graphBuilder.buildGraph(tables);

            //seek an event node and find descendants
            Map<String, Table> tablesMap = graphBuilder.getStringTableMap(tables);

            //prepare commands for sql processing
            List<SelectionData> selectionDataSet = graphToCommandsService
                .prepareCommands(startingNode, graph, tablesMap);

            final List<com.codeplus.digger.core.api.SelectionData> selectionDataApi = transform2Api(
                selectionDataSet);

            final List<Script> sqlCommands = ids.flatMap(id ->
                commandsToScripts.generateScript(selectionDataApi, id)
            ).toList();

            sqlCommands.forEach(
                System.out::println
            );

//            System.out.println(sqlCommands);

            //prepare sql to load data from database

            //and create sql insert script
            //push to file
            //build docker image or docker file

        } catch (Exception ex) {
            log.error("Unknown error", ex);
        }
    }

    private List<com.codeplus.digger.core.api.SelectionData> transform2Api(
        List<SelectionData> selectionDataSet) {
        return selectionDataSet
            .map(
                sd -> com.codeplus.digger.core.api.SelectionData.builder()
                    .fromTable(com.codeplus.digger.core.api.Table.builder()
                        .dataColumns(
                            sd.getFromTable().getDataColumns().map(dc ->
                                com.codeplus.digger.core.api.Column.builder()
                                    .name(dc.getName()).build()).toList()
                        )
                        .referenceColumns(
                            sd.getFromTable().getReferenceColumns().map(rc ->
                                ReferenceColumn.builder()
                                    .name(rc.getName())
                                    .destinationTableName(rc.getName().replace("FK", ""))
                                    .build())
                        )
                        .name(sd.getFromTable().getName())
                        .build()
                    )
                    .byColumn(sd.getByColumn())
                    .whereColumn(sd.getWhereColumn())
                    .toTable(
                        com.codeplus.digger.core.api.Table.builder()
                            .name(sd.getToTable().getName())
                            .dataColumns(
                                sd.getToTable().getDataColumns().map(dc ->
                                    com.codeplus.digger.core.api.Column.builder()
                                        .name(dc.getName())
                                        .build())
                                    .toList())
                            .referenceColumns(
                                sd.getToTable().getReferenceColumns().map(rc ->
                                    ReferenceColumn.builder()
                                        .name(rc.getName())
                                        .destinationTableName(rc.getName().replace("FK", ""))
                                        .build()
                                ).toList()
                            )
                            .build()
                    )
                    .build()
            ).toList();
    }

    private List<Table> mapApiToInternalTables(TablesSupplier tableSupplier) {

        final Stream<com.codeplus.digger.core.api.Table> javaStream = StreamSupport
            .stream(tableSupplier, NO_PARALLEL);
        return io.vavr.collection.Stream.ofAll(javaStream)
            .map(
                apiTable -> {
                    List<Column> columns = apiTable.getDataColumns()
                        .map(c -> Column.builder().name(c.getName()).build())
                        .toList();

                    List<Column> refsColumns = apiTable.getReferenceColumns()
                        .map(c -> Column.builder().name(c.getName()).build())
                        .toList();

                    return Table.builder().name(apiTable.getName())
                        .dataColumns(columns)
                        .referenceColumns(refsColumns)
                        .build();
                }
            ).toList();
    }

    private static final class FileTablesSupplier implements TablesSupplier {

        private Stack<com.codeplus.digger.core.api.Table> tableStack = new Stack<>();

        public FileTablesSupplier() {
            List<com.codeplus.digger.core.api.Table> tables = loadFromFile();
            tables.forEach(t -> tableStack.push(t));
        }

        private List<com.codeplus.digger.core.api.Table> loadFromFile() {
            final List<com.codeplus.digger.core.api.Table> tables = List.empty();

            try (BufferedReader reader = Files.newBufferedReader(
                Paths.get("./tables.json")
            )) {

                GsonBuilder builder = new GsonBuilder();
                VavrGson.registerAll(builder);
                final Gson gson = builder.create();
                Type type = new TypeToken<List<com.codeplus.digger.core.api.Table>>() {
                }.getType();

                List<com.codeplus.digger.core.api.Table> tablesArr = gson
                    .fromJson(reader, type);

                return tablesArr;
            } catch (Exception ex) {
                log.error("Cant load tables from file", ex);
            }
            return tables;
        }

        @Override
        public boolean tryAdvance(Consumer<? super com.codeplus.digger.core.api.Table> action) {
            if (tableStack.isEmpty()) {
                return false;
            } else {
                action.accept(tableStack.pop());
                return true;
            }

        }

        @Override
        public Spliterator<com.codeplus.digger.core.api.Table> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return tableStack.size();
        }

        @Override
        public int characteristics() {
            return SIZED;
        }
    }

    private void saveToFile(List<com.codeplus.digger.core.api.Table> tables) {

        try (BufferedWriter bufferedWriter = Files
            .newBufferedWriter(Paths.get("./tables.json"),
                StandardOpenOption.CREATE_NEW
            )) {

            final String json = new Gson().toJson(tables);

            bufferedWriter.write(json);

        } catch (Exception ex) {
            log.error("Can't create file", ex);
        }

    }

}
