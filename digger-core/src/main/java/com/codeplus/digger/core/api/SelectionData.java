package com.codeplus.digger.core.api;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SelectionData {

    private Table fromTable;
    private String byColumn;
    private String whereColumn;
    private Table toTable;
}
