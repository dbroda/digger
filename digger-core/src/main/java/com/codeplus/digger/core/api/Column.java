package com.codeplus.digger.core.api;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Column contains data about {@link Table table's} columns
 * Used to perform select and insert queries
 * @see com.codeplus.digger.core.api.Table
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public final class Column {

    private String name;
}
