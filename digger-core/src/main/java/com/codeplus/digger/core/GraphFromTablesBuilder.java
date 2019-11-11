package com.codeplus.digger.core;


import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.google.common.graph.MutableValueGraph;
import com.google.common.graph.ValueGraph;
import com.google.common.graph.ValueGraphBuilder;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class GraphFromTablesBuilder {

    public MutableValueGraph<Table, Column> buildGraph(List<Table> tables) {

        final MutableValueGraph<Table, Column> graph = buildGraph();

        Map<String, Table> tablesMap = getStringTableMap(tables);

        addNodesToGraph(tables, graph, tablesMap);

        prepareStaticalReferences(graph, tablesMap);

        return graph;

    }

    public Map<String, Table> getStringTableMap(List<Table> tables) {
        return tables.toMap(Table::getName, i -> i);
    }


    private void addNodesToGraph(List<Table> tables, MutableValueGraph<Table, Column> mutableGraph,
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
                    t -> {
                        final Column fk =   table.getReferenceColumns()
                            .find(c -> c.getName().replace("FK", "").equalsIgnoreCase(t.getName()))
                            .get();

                        mutableGraph.putEdgeValue(table, t, fk);

                    }

                );
            }
        );
    }

    private MutableValueGraph<Table, Column> buildGraph() {
        return ValueGraphBuilder.directed().allowsSelfLoops(true).build();
    }

    private void prepareStaticalReferences(MutableValueGraph<Table, Column> mutableGraph,
        Map<String, Table> tablesMap) {

//TODO add object objectFK tables
//        mutableGraph.putEdge(tablesMap.get("outcome").get(), tablesMap.get("event").get());
//        mutableGraph.putEdge(tablesMap.get("property").get(), tablesMap.get("incident").get());
//        mutableGraph.putEdge(tablesMap.get("property").get(), tablesMap.get("event").get());
//        mutableGraph.putEdge(tablesMap.get("property").get(), tablesMap.get("event_participants").get());
//        mutableGraph.putEdge(tablesMap.get("property").get(), tablesMap.get("participant").get());
//        mutableGraph.putEdge(tablesMap.get("property").get(), tablesMap.get("tournament_stage").get());
//        mutableGraph.putEdge(tablesMap.get("property").get(), tablesMap.get("object_participants").get());
//        mutableGraph.putEdge(tablesMap.get("property").get(), tablesMap.get("lineup").get());
//        mutableGraph.putEdge(tablesMap.get("property").get(), tablesMap.get("standing_participants").get());

    }


}
