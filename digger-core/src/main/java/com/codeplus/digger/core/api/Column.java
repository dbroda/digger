package com.codeplus.digger.core.api;

import lombok.Builder;
import lombok.Data;

/**
 * Column contains data about {@link Table table's} columns
 * Used to perform select and insert queries
 * @see com.codeplus.digger.core.api.Table
 */
@Data
@Builder
public final class Column {

    private String name;
}
