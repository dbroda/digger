package com.codeplus.digger.core;

import com.google.common.collect.TreeTraverser;
import com.google.common.graph.MutableGraph;
import io.vavr.collection.Array;
import io.vavr.collection.HashSet;
import io.vavr.collection.LinkedHashSet;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Set;
import io.vavr.collection.Stream;
import io.vavr.collection.TreeSet;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class GraphToCommandsService {

    public List<SelectionData> prepareCommands(String startingNode,
        MutableGraph<Table> graph,
        Map<String, Table> tablesMap) {

        final Table fromTable = tablesMap.get(startingNode).get();
        SelectionData primal = SelectionData.builder()
            .toTable(fromTable)
            .fromTable(fromTable)
            .byColumn("id")
            .whereColumn("id")
            .build();

        final List<SelectionData> linkedHashSet = List.of(primal);

        final List<SelectionData> selectionData = prepareSelectionForNode(startingNode, graph,
            tablesMap);

        selectionData.
            map(sd -> sd.getToTable().getName())
            .forEach(sd -> log.info("Loading dependency {}", sd));


        return linkedHashSet.appendAll(selectionData);


//
//        ImmutableList<Table> tables1 = tables.toList();
//
//        log.info("{}", tables1);
//        TreeTraverser<Table> tt2 = TreeTraverser.using(t -> graph.predecessors(t));
//
//        FluentIterable<Table> tables2 = tt2.breadthFirstTraversal(eventNode);
//
//        ImmutableList<Table> tables3 = tables2.toList();
//
//        log.info("{}", tables3);
//
//        return new HashSet<>();

    }

    private List<SelectionData> prepareSelectionForNode(String startingNode,
        MutableGraph<Table> graph, Map<String, Table> tablesMap) {
        Table eventNode = tablesMap.get(startingNode).get();

        TreeTraverser<Table> tt = TreeTraverser.using(graph::successors);

//        final SelectionData selfSelection = SelectionData.builder()
//            .byColumn("id")
//            .fromTable(eventNode)
//            .toTable(eventNode)
//            .build();

        final List<SelectionData> collect = Stream.ofAll(tt.breadthFirstTraversal(eventNode)
            .stream())
            .filter(t -> graph.hasEdgeConnecting(eventNode, t))
            .map(t -> SelectionData.builder()
                .toTable(t)
                .fromTable(eventNode)
                .byColumn(t.getName() + "FK")
                .whereColumn("id")
                .build()
            ).toList();

        final List<SelectionData> priorTables = collect
            .flatMap(
                t -> prepareSelectionForNode(t.getToTable().getName(), graph, tablesMap))
            .toList();

        return  collect.appendAll(priorTables);

//        collect.add(selfSelection);


    }

    private void findTablesWithObjectColumns(List<Table> tables) {
        tables.forEach(
            table -> {
                boolean matched = table.getDataColumns().
                    exists(c -> c.getName().equalsIgnoreCase("object"));

                if (matched) {
                    log.info("Table {} contains object!", table.getName());
                }
            }
        );
    }

//    private Set<Table> findAllRelations(final MutableGraph<Table> mutableGraph, Table table,
//        Set<Table> accumulator) {
//
//        log.info("Digging into table {}", table.getName());
//        Set<Table> relatedSet = new HashSet<>();
//
//        accumulator.add(table);
//
//        relatedSet.add(table);
//
//        Set<Table> successors = mutableGraph.successors(table);
//        Set<Table> predecessors = mutableGraph.predecessors(table);
//
//        Set<Table> related =
//            Stream.concat(successors.stream(), predecessors.stream())
//                .filter(s -> !accumulator.contains(s))
//                .flatMap(s -> findAllRelations(mutableGraph, s, accumulator).stream())
//                .collect(Collectors.toSet());
//
//        relatedSet.addAll(successors);
//        relatedSet.addAll(predecessors);
//
//        relatedSet.addAll(related);
//
//        return relatedSet;
//    }

//    private List<SelectionData> getSelectionData(MutableGraph<Table> mutableGraph,
//        Table eventNode) {
//
//        List<SelectionData> selectionData = getTargetSql(mutableGraph, eventNode);
//
//        List<SelectionData> collectSource = getSourceSql(mutableGraph, eventNode);
//
//        selectionData.addAll(collectSource);
//
//        return selectionData.stream().distinct().collect(Collectors.toList());
//
////        List<SelectionData> fixedSelection = selectionData.stream()
////            .filter(sd -> sd.getFromTable().getReferenceColumns().stream()
////                .noneMatch(c -> c.getName().equalsIgnoreCase(sd.getWhereColumn())))
////            .map(sd -> {
////                sd.setWhereColumn( "objectFK");
////                return sd;
////            }).collect(toList());
////
////        selectionData.addAll(fixedSelection);
//
//    }

//    private List<SelectionData> getSourceSql(MutableGraph<Table> mutableGraph, Table contextNode) {
//        return mutableGraph.edges().stream()
//            .filter(n -> n.source() == contextNode)
//            .map(n -> SelectionData.builder().fromTable(n.source())
//                .whereColumn("id")
//                .contextTable(contextNode).build())
//            .collect(toList());
//    }
//
//    private List<SelectionData> getTargetSql(MutableGraph<Table> mutableGraph, Table contextNode) {
//        return mutableGraph.edges().stream()
//            .filter(n -> n.target() == contextNode)
//            .map(n -> SelectionData.builder().fromTable(n.source())
//                .whereColumn(n.target().getName() + "FK")
//                .contextTable(contextNode).build())
//            .collect(toList());
//    }


}
