package com.codeplus.digger.core;

import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Builder
@Data
class Table implements Comparable<Table> {

    private String name;

    @Builder.Default
    private List<Column> referenceColumns = new ArrayList<>();

    @Builder.Default
    private List<Column> dataColumns = new ArrayList<>();

    @Override
    public int compareTo(Table table) {
        return this.getName().compareTo(table.getName());
    }
}
