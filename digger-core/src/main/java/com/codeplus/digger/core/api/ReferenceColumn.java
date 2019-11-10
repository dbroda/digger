package com.codeplus.digger.core.api;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


/***
 * A column that contains reference to other {@link Table}
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ReferenceColumn {

    private String name;
    private String destinationTableName;
}
