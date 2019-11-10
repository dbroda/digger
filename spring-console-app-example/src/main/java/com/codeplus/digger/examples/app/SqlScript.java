package com.codeplus.digger.examples.app;

import com.codeplus.digger.core.api.Script;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@ToString
@Getter
class SqlScript implements Script {

    private String sql;
}
