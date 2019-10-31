package com.codeplus.digger.core;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.google.common.graph.ElementOrder;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import java.util.List;
import java.util.Map;
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
        return tables.stream().collect(
            toMap(Table::getName, i -> i)
        );
    }


    private void addNodesToGraph(List<Table> tables, MutableGraph<Table> mutableGraph,
        Map<String, Table> tablesMap) {
        tables.stream().forEach(
            table -> {

                mutableGraph.addNode(table);
                List<Table> collectedTablesFromColumns = table.getReferenceColumns().stream()
                    .map(Column::getName)
                    .map(name -> tablesMap.get(name))
                    .collect(toList());

                collectedTablesFromColumns.stream().forEach(
                    t -> mutableGraph.putEdge(table, t)

                );
            }
        );
    }

    private MutableGraph<Table> buildGraph() {
        GraphBuilder graphBuilder = GraphBuilder.directed();

        //tables are self referenced
        graphBuilder.allowsSelfLoops(true);
        ElementOrder<?> natural = ElementOrder.natural();
        graphBuilder.nodeOrder(natural);

        return graphBuilder.build();
    }

    private void prepareStaticalReferences(MutableGraph<Table> mutableGraph,
        Map<String, Table> tablesMap) {

        mutableGraph.putEdge(tablesMap.get("outcome"), tablesMap.get("event"));
        mutableGraph.putEdge(tablesMap.get("property"), tablesMap.get("incident"));
        mutableGraph.putEdge(tablesMap.get("property"), tablesMap.get("event"));
        mutableGraph.putEdge(tablesMap.get("property"), tablesMap.get("event_participants"));
        mutableGraph.putEdge(tablesMap.get("property"), tablesMap.get("participant"));
        mutableGraph.putEdge(tablesMap.get("property"), tablesMap.get("tournament_stage"));
        mutableGraph.putEdge(tablesMap.get("property"), tablesMap.get("object_participants"));
        mutableGraph.putEdge(tablesMap.get("property"), tablesMap.get("lineup"));
        mutableGraph.putEdge(tablesMap.get("property"), tablesMap.get("standing_participants"));

    }


}
