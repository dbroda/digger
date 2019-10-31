package com.codeplus.digger.core;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
class SelectionData {

    private Table fromTable;
    private String whereColumn;
    private Table contextTable;
}
