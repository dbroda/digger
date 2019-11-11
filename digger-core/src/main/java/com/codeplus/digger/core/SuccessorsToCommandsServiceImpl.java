package com.codeplus.digger.core;

import com.google.common.collect.TreeTraverser;
import com.google.common.graph.MutableGraph;
import com.google.common.graph.MutableValueGraph;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class SuccessorsToCommandsServiceImpl extends GraphToCommandsServiceAbstract {

    @Override
    protected String nextTableName(SelectionData t)  {
        return t.getToTable().getName();
    }

    @Override
    protected Predicate<Table> filterEdges(MutableValueGraph<Table, Column> graph, Table tableNode) {
        return t -> graph.hasEdgeConnecting(tableNode, t);
    }

    @Override
    protected TreeTraverser<Table> getTableTreeTraverser(MutableValueGraph<Table, Column> graph) {
        return TreeTraverser.using(graph::successors);
    }

    @Override
    protected SelectionData buildSelection(Table diagnosedNode, Table traversed, Column column) {
        return SelectionData.builder()
            .toTable(traversed)
            .fromTable(diagnosedNode)
            .byColumn(traversed.getName() + "FK")
            .whereColumn("id")
            .build();
    }

}
