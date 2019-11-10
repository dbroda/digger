package com.codeplus.digger.core;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
class SelectionData implements Comparable<SelectionData> {

    private Table fromTable;
    private String byColumn;
    private String whereColumn;
    private Table toTable;

    @Override
    public int compareTo(SelectionData o) {
        return fromTable.compareTo(o.fromTable);
    }
}
