package com.codeplus.digger.examples.app;

import com.codeplus.digger.core.api.Column;
import com.codeplus.digger.core.api.CommandsToScript;
import com.codeplus.digger.core.api.ReferenceColumn;
import com.codeplus.digger.core.api.Script;
import com.codeplus.digger.core.api.SelectionData;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.control.Option;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

@AllArgsConstructor
@Slf4j
class SqlCommandsToScripts implements CommandsToScript<LongID> {

    private JdbcTemplate jdbcTemplate;


    @Data
    @Builder
    @AllArgsConstructor
    private static class SelectionDataWithId {

        private SelectionData selectionData;
        private LongID longID;
        @Default
        private List<Script> scripts = List.empty();
    }

    @Override
    public List<Script> generateScript(List<SelectionData> selectionDataSet1, LongID id) {

        Objects.requireNonNull(selectionDataSet1, "Data set can be null");

        if (selectionDataSet1.isEmpty()) {
            return List.empty();
        }

        final List<SelectionDataWithId> selectionDataWithIds = selectionDataSet1
            .map(sd -> SelectionDataWithId.builder().selectionData(sd).build()).toList();

        final Option<SelectionDataWithId> optionalHead = selectionDataWithIds.find(
            sdw -> sdw.getSelectionData().getByColumn().equalsIgnoreCase("id") &&
                sdw.getSelectionData().getFromTable().getName()
                    .equals(sdw.getSelectionData().getToTable().getName())
        );

        if (optionalHead.isEmpty()) {
            log.warn("Cant find main object to traverse (selection by self referenced id)");
            return List.empty();
        }

        final SelectionDataWithId firstSelection = optionalHead.get();

        firstSelection.setLongID(id);

        while (readyToProcess(selectionDataWithIds).nonEmpty()) {

            final List<SelectionDataWithId> readyList = readyToProcess(
                selectionDataWithIds);

            readyList.forEach(selectionDataWithId -> {

                    final SelectionData sd = selectionDataWithId.getSelectionData();
                    final List<RowResult> rowResults = loadDataFromDs(
                        sd, selectionDataWithId.getLongID());

                    final List<Script> sqlScripts = rowResults.map(this::generateInsertSql)
                        .map(SqlScript::new)
                        .map(x -> (Script) x)
                        .toList();

                    selectionDataWithId.setScripts(sqlScripts);

                    for (ReferenceColumn rc : sd.getToTable().getReferenceColumns()) {
                        final String valS = rowResults
                            .filter(rr -> rr.data.containsKey(rc.getName()))
                            .map(rr -> rr.data.get(rc.getName()).get()).head() + "";

                        final LongID longID = new LongID(Long.parseLong(valS));

                        //mutable op
                        selectionDataWithIds.find(sdi -> sdi.getSelectionData().getToTable().getName()
                            .equalsIgnoreCase(rc.getDestinationTableName()))
                            .forEach(sdi -> sdi.setLongID(longID));
                    }
                }
            );

        }
        final List<Script> scripts = selectionDataWithIds.flatMap(sdi -> sdi.getScripts()).toList();

        return scripts;


    }

    private List<SelectionDataWithId> readyToProcess(
        List<SelectionDataWithId> selectionDataWithIds) {
        return selectionDataWithIds
            .filter(dwq -> dwq.getLongID() != null && dwq.getScripts().isEmpty())
            .toList();
    }

    private String generateInsertSql(RowResult rowResult) {
        final String columns = rowResult.data.map(t2 -> t2._1).mkString(",");
        final String values = rowResult.data.map(t2 -> t2._2 == null ? "null" :  "\"" + t2._2.toString() + "\"").mkString(",");
        final String sql = String
            .format("INSERT INTO %s(%s) VALUES(%s);", rowResult.name, columns, values);

        return sql;

    }

    private List<RowResult> loadDataFromDs(SelectionData selectionData, LongID id) {

        final String columns = selectionData.getToTable().getDataColumns().map(Column::getName)
            .mkString(",");
        final String sql = String
            .format("SELECT %s FROM %s WHERE %s=?", columns, selectionData.getToTable().getName(),
                selectionData.getWhereColumn());

        final List<RowResult> dataResults = List.ofAll(jdbcTemplate
            .queryForList(sql, new Object[]{id.getValue()})).map(
            HashMap::ofAll)
            .map(m -> RowResult.builder().name(selectionData.getToTable().getName())
                .data(m).build())
            .toList();

        return dataResults;

    }

    @Getter
    @ToString
    @Builder
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    private static class RowResult {

        private String name;
        @Default
        private Map<String, Object> data = HashMap.empty();
    }
}
