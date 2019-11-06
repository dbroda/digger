package com.codeplus.digger.core;

import com.codeplus.digger.core.api.TablesSupplier;
import com.google.common.graph.MutableGraph;
import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class Digger {

    public static Digger instance() {
        return new Digger(
            new GraphFromTablesBuilder(),
            new GraphToCommandsService()
        );
    }

    private GraphFromTablesBuilder graphBuilder;
    private GraphToCommandsService graphToCommandsService;

    Digger(GraphFromTablesBuilder graphBuilder,
        GraphToCommandsService graphToCommandsService) {
        this.graphBuilder = graphBuilder;
        this.graphToCommandsService = graphToCommandsService;
    }

    public void createScript(TablesSupplier tableSupplier) {

        try {

            final List<Table> tables = mapApiToInternalTables(tableSupplier);

//            findTablesWithObjectColumns(tables);

            //prepare graph
            MutableGraph<Table> graph = graphBuilder.buildGraph(tables);

            //seek an event node and find descendants
            Map<String, Table> tablesMap = graphBuilder.getStringTableMap(tables);

            //prepare commands for sql processing
            Set<SelectionData> selectionDataSet = graphToCommandsService
                .prepareCommands(graph, tablesMap);

            System.out.println(selectionDataSet);

            //prepare sql to load data from database

            Map<String, SelectionData> collect = selectionDataSet.stream()
                .collect(Collectors.toList()
                ).stream()
                .collect(Collectors.toMap(
                    sd -> sd.getFromTable().getName(),
                    i -> i,
                    (a, b) -> a
                ));

            //and create sql insert script
            //push to file
            //build docker image?

        } catch (Exception ex) {
            log.error("Unknown error", ex);
        }
    }

    private List<Table> mapApiToInternalTables(TablesSupplier tableSupplier) {
        return Stream.generate(tableSupplier)
                    .map(
                        apiTable -> {
                            List<Column> columns = apiTable.getDataColumns().stream()
                                .map(c -> Column.builder().name(c.getName()).build())
                                .collect(Collectors.toList());

                            List<Column> refsColumns = apiTable.getReferenceColumns().stream()
                                .map(c -> Column.builder().name(c.getName()).build())
                                .collect(Collectors.toList());

                            return Table.builder().name(apiTable.getName())
                                .dataColumns(columns)
                                .referenceColumns(refsColumns)
                                .build();
                        }
                    ).collect(Collectors.toList());
    }

    private List<Table> loadFromFile() throws IOException {
        final List<Table> tables = new ArrayList<>();

        //databaseSchemaLoader.loadSchema();

        try (BufferedReader reader = Files.newBufferedReader(
            Paths.get("/home/dbroda/workspace/kata/tables.json")
        )) {

            Table[] tablesArr = new Gson().fromJson(reader, Table[].class);

            tables.addAll(Arrays.asList(tablesArr));
        }
        return tables;
    }


}
