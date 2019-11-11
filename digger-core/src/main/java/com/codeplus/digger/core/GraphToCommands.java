package com.codeplus.digger.core;

import com.google.common.graph.MutableValueGraph;
import io.vavr.collection.List;
import io.vavr.collection.Map;

interface GraphToCommands {

    List<SelectionData> prepareCommands(String startingNode,
        MutableValueGraph<Table, Column> graph,
        Map<String, Table> tablesMap);
}
