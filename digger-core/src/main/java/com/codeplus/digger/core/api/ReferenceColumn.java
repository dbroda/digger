package com.codeplus.digger.core.api;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ReferenceColumn {

    private String name;
    private String destinationTableName;
}
