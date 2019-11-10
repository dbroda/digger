package com.codeplus.digger.core;


import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class GraphFromTablesBuilder {

    public MutableGraph<Table> buildGraph(List<Table> tables) {

        final MutableGraph<Table> mutableGraph = buildGraph();

        Map<String, Table> tablesMap = getStringTableMap(tables);

        addNodesToGraph(tables, mutableGraph, tablesMap);

        prepareStaticalReferences(mutableGraph, tablesMap);

        return mutableGraph;

    }

    public Map<String, Table> getStringTableMap(List<Table> tables) {
        return tables.toMap(Table::getName, i -> i);
    }


    private void addNodesToGraph(List<Table> tables, MutableGraph<Table> mutableGraph,
        Map<String, Table> tablesMap) {
        tables.forEach(
            table -> mutableGraph.addNode(table)
        );

        tables.forEach(
            table -> {
                log.info("Processing {} table", table);
                List<Table> collectedTablesFromColumns = table.getReferenceColumns()
                    .map(Column::getName)
                    .map(c -> c.replace("FK", ""))
                    .filter(tablesMap::containsKey)
                    .map(name -> tablesMap.get(name).get())
                    .toList();

                log.info("collectedTablesFromColumns {} ", collectedTablesFromColumns);

                collectedTablesFromColumns.forEach(
                    t -> mutableGraph.putEdge(table, t)

                );
            }
        );
    }

    private MutableGraph<Table> buildGraph() {
        GraphBuilder graphBuilder = GraphBuilder.directed();

        //tables are self referenced
        graphBuilder.allowsSelfLoops(true);

        return graphBuilder.build();
    }

    private void prepareStaticalReferences(MutableGraph<Table> mutableGraph,
        Map<String, Table> tablesMap) {

        mutableGraph.putEdge(tablesMap.get("outcome").get(), tablesMap.get("event").get());
        mutableGraph.putEdge(tablesMap.get("property").get(), tablesMap.get("incident").get());
        mutableGraph.putEdge(tablesMap.get("property").get(), tablesMap.get("event").get());
        mutableGraph.putEdge(tablesMap.get("property").get(), tablesMap.get("event_participants").get());
        mutableGraph.putEdge(tablesMap.get("property").get(), tablesMap.get("participant").get());
        mutableGraph.putEdge(tablesMap.get("property").get(), tablesMap.get("tournament_stage").get());
        mutableGraph.putEdge(tablesMap.get("property").get(), tablesMap.get("object_participants").get());
        mutableGraph.putEdge(tablesMap.get("property").get(), tablesMap.get("lineup").get());
        mutableGraph.putEdge(tablesMap.get("property").get(), tablesMap.get("standing_participants").get());

    }


}
