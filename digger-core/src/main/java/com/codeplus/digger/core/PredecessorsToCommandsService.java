package com.codeplus.digger.core;

import com.google.common.collect.TreeTraverser;
import com.google.common.graph.MutableGraph;
import com.google.common.graph.MutableValueGraph;
import java.util.function.Predicate;

class PredecessorsToCommandsService extends GraphToCommandsServiceAbstract {

    @Override
    protected String nextTableName(SelectionData t) {
        return t.getFromTable().getName();
    }

    @Override
    protected Predicate<Table> filterEdges(MutableValueGraph<Table, Column> graph, Table tableNode) {
        return t -> graph.hasEdgeConnecting(t, tableNode) && t.getName() != tableNode.getName();
        //return __ -> !__.getName().equalsIgnoreCase(tableNode.getName());
    }

    @Override
    protected TreeTraverser<Table> getTableTreeTraverser(MutableValueGraph<Table, Column> graph) {
        return TreeTraverser.using(graph::predecessors);
    }

    @Override
    protected SelectionData buildSelection(Table diagnosedNode, Table traversed, Column c) {

        String columnFK = c.getName();

        if (diagnosedNode == traversed) {
            columnFK = "id";
        }

        return SelectionData.builder()
            .toTable(diagnosedNode)
            .fromTable(traversed)
            .byColumn(columnFK)
            .whereColumn(columnFK)
            .build();
    }
}
