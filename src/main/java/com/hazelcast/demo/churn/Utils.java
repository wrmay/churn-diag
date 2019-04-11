package com.hazelcast.demo.churn;

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.function.PredicateEx;

public class Utils {
	public static <T, A, R> AggregateOperation1<T, A, R> filtering(
             PredicateEx<? super T> filterFn,
             AggregateOperation1<? super T, A, ? extends R> downstream
    ) {
        BiConsumerEx<? super A, ? super T> downstreamAccumulateFn = downstream.accumulateFn();
        return AggregateOperation
                .withCreate(downstream.createFn())
                .andAccumulate((A a, T t) -> {
                    if (filterFn.test(t)) {
                        downstreamAccumulateFn.accept(a, t);
                    }
                })
                .andCombine(downstream.combineFn())
                .andDeduct(downstream.deductFn())
                .<R>andExport(downstream.exportFn())
                .andFinish(downstream.finishFn());
    }
}
