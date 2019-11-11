package com.codeplus.digger.core;

import com.google.common.graph.MutableGraph;
import com.google.common.graph.MutableValueGraph;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import lombok.AllArgsConstructor;

@AllArgsConstructor
class GraphToCommandsSequence implements GraphToCommands {

    private List<GraphToCommands> graphToCommands;


    @Override
    public List<SelectionData> prepareCommands(String startingNode, MutableValueGraph<Table, Column> graph,
        Map<String, Table> tablesMap) {

        return graphToCommands.flatMap(
            gtc ->  gtc.prepareCommands(startingNode, graph, tablesMap)
        ).toList();

    }
}
