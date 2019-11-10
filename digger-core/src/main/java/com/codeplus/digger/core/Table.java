package com.codeplus.digger.core;

import io.vavr.collection.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
class Table implements Comparable<Table> {

    private String name;

    @Builder.Default
    private List<Column> referenceColumns = List.empty();

    @Builder.Default
    private List<Column> dataColumns = List.empty();

    @Override
    public int compareTo(Table table) {
        return this.getName().compareTo(table.getName());
    }
}
