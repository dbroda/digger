package com.codeplus.digger.core;

import com.google.common.collect.TreeTraverser;
import com.google.common.graph.MutableValueGraph;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class GraphToCommandsServiceAbstract implements GraphToCommands {

    public List<SelectionData> prepareCommands(String startingNode,
        MutableValueGraph<Table, Column> graph,
        Map<String, Table> tablesMap) {

        final Table fromTable = tablesMap.get(startingNode).get();
        final Column id = fromTable.getDataColumns()
            .find(c -> c.getName().equalsIgnoreCase("id"))
            .get();
        SelectionData primal = buildSelection(fromTable, fromTable,
            id);

        final List<SelectionData> linkedHashSet = List.of(primal);

        final List<SelectionData> selectionData = prepareSelectionForNode(startingNode, graph,
            tablesMap);

        selectionData.
            map(sd -> sd.getToTable().getName())
            .forEach(sd -> log.info("Loading dependency {}", sd));

        return linkedHashSet.appendAll(selectionData);


    }

    private List<SelectionData> prepareSelectionForNode(String startingNode,
        MutableValueGraph<Table, Column> graph, Map<String, Table> tablesMap) {
        Table tableNode = tablesMap.get(startingNode).get();

        TreeTraverser<Table> tt = getTableTreeTraverser(graph);

        final List<SelectionData> collect = Stream.ofAll(tt.breadthFirstTraversal(tableNode)
            .stream())
            .filter(filterEdges(graph, tableNode))
            .map(t -> buildSelection(tableNode, t, graph.edgeValue(tableNode,t).get())
            ).toList();

        final List<SelectionData> priorTables = collect
            .flatMap(
                t -> prepareSelectionForNode(nextTableName(t), graph, tablesMap))
            .toList();

        return collect.appendAll(priorTables);

//        collect.add(selfSelection);

    }

    protected abstract String nextTableName(SelectionData t);

    protected abstract Predicate<Table> filterEdges(MutableValueGraph<Table, Column> graph,
        Table tableNode);

    protected abstract TreeTraverser<Table> getTableTreeTraverser(
        MutableValueGraph<Table, Column> graph);
//    {
//        return TreeTraverser.using(graph::successors);
//    }

    protected abstract SelectionData buildSelection(Table diagnosedNode, Table traversed,
        Column column);

    {
//        return SelectionData.builder()
//            .toTable(traversed)
//            .fromTable(diagnosedNode)
//            .byColumn(byColumn)
//            .whereColumn("id")
//            .build();
//    }

//    private void findTablesWithObjectColumns(List<Table> tables) {
//        tables.forEach(
//            table -> {
//                boolean matched = table.getDataColumns().
//                    exists(c -> c.getName().equalsIgnoreCase("object"));
//
//                if (matched) {
//                    log.info("Table {} contains object!", table.getName());
//                }
//            }
//        );
//    }

    }
}
