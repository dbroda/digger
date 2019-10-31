package com.codeplus.digger.core;

import static java.util.stream.Collectors.toList;

import com.google.common.graph.MutableGraph;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class GraphToCommandsService {

    public Set<SelectionData> prepareCommands(MutableGraph<Table> graph,
        Map<String, Table> tablesMap) {
        Table eventNode = tablesMap.get("event");

        Set<Table> allRelations = findAllRelations(graph, eventNode, new HashSet<>());

        Set<SelectionData> collect = allRelations.stream()
            .flatMap(t -> getSelectionData(graph, t).stream())
            .collect(Collectors.toSet());

        return collect;

    }

    private void findTablesWithObjectColumns(List<Table> tables) {
        tables.stream().forEach(
            table -> {
                boolean matched = table.getDataColumns().stream()
                    .anyMatch(c -> c.getName().equalsIgnoreCase("object"));

                if (matched) {
                    log.info("Table {} contains object!", table.getName());
                }
            }
        );
    }

    private Set<Table> findAllRelations(final MutableGraph<Table> mutableGraph, Table table,
        Set<Table> accumulator) {

        log.info("Digging into table {}", table.getName());
        Set<Table> relatedSet = new HashSet<>();

        accumulator.add(table);

        relatedSet.add(table);

        Set<Table> successors = mutableGraph.successors(table);
        Set<Table> predecessors = mutableGraph.predecessors(table);

        Set<Table> related =
            Stream.concat(successors.stream(), predecessors.stream())
                .filter(s -> !accumulator.contains(s))
                .flatMap(s -> findAllRelations(mutableGraph, s, accumulator).stream())
                .collect(Collectors.toSet());

        relatedSet.addAll(successors);
        relatedSet.addAll(predecessors);

        relatedSet.addAll(related);

        return relatedSet;
    }

    private List<SelectionData> getSelectionData(MutableGraph<Table> mutableGraph,
        Table eventNode) {
        List<SelectionData> selectionData = getTargetSql(mutableGraph, eventNode);

        List<SelectionData> collectSource = getSourceSql(mutableGraph, eventNode);

        selectionData.addAll(collectSource);

        List<SelectionData> fixedSelection = selectionData.stream()
            .filter(sd -> sd.getFromTable().getReferenceColumns().stream()
                .noneMatch(c -> c.getName().equalsIgnoreCase(sd.getWhereColumn())))
            .map(sd -> {
                sd.setWhereColumn( "objectFK");
                return sd;
            }).collect(toList());

        selectionData.addAll(fixedSelection);
        return selectionData;
    }

    private List<SelectionData> getSourceSql(MutableGraph<Table> mutableGraph, Table contextNode) {
        return mutableGraph.edges().stream()
            .filter(n -> n.source() == contextNode)
            .map(n -> SelectionData.builder().fromTable(n.source())
                .whereColumn(n.target().getName())
                .contextTable(contextNode).build())
            .collect(toList());
    }

    private List<SelectionData> getTargetSql(MutableGraph<Table> mutableGraph, Table contextNode) {
        return mutableGraph.edges().stream()
            .filter(n -> n.target() == contextNode)
            .map(n -> SelectionData.builder().fromTable(n.source())
                .whereColumn(n.target().getName())
                .contextTable(contextNode).build())
            .collect(toList());
    }




}
