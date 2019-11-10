package com.codeplus.digger.examples.app;

import com.codeplus.digger.core.api.ID;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
class LongID implements ID<Long> {

    private Long value;

}
