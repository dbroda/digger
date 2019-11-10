package com.codeplus.digger.core.api;

import io.vavr.collection.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/***
 * Table that corresponds to tables in sql db manner
 */
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public final class Table implements Comparable<Table>{

    private String name;

    @Builder.Default()
    private List<ReferenceColumn> referenceColumns = List.empty();

    @Builder.Default
    private List<Column> dataColumns = List.empty();

    @Override
    public int compareTo(Table o) {
        return name.compareTo(o.name);
    }
}
