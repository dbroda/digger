package com.codeplus.digger.core.api;

import lombok.Builder;
import lombok.Data;


/***
 * A column that contains reference to other {@link Table}
 */
@Data
@Builder
public class ReferenceColumn {

    private String name;
    private String destinationTableName;
}
