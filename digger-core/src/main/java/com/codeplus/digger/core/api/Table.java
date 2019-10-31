package com.codeplus.digger.core.api;

import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
public final class Table {

    private String name;

    @Builder.Default
    private List<ReferenceColumn> referenceColumns = new ArrayList<>();

    @Builder.Default
    private List<Column> dataColumns = new ArrayList<>();

}
