package com.codeplus.digger.core.api;


import io.vavr.collection.List;
import io.vavr.collection.Set;

public interface CommandsToScript<T extends ID> {

    List<Script> generateScript(List<SelectionData> selectionDataSet, T id);
}
