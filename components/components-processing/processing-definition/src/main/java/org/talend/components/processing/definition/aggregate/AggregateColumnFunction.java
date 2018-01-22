package org.talend.components.processing.definition.aggregate;

import java.util.Arrays;
import java.util.List;

public enum AggregateColumnFunction {
    LIST,
    COUNT,
    SUM,
    AVG,
    MIN,
    MAX;

    public static final List<AggregateColumnFunction> NUMERICAL_FUNCTIONS =
            Arrays.asList(SUM, AVG, MIN, MAX, COUNT, LIST);

    public static final List<AggregateColumnFunction> NON_NUMERICAL_FUNCTIONS = Arrays.asList(COUNT, LIST);
}
