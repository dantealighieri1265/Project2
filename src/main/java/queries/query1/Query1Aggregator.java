package queries.query1;

import utils.ShipData;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.Map;

public class Query1Aggregator implements AggregateFunction<ShipData, Query1Accumulator, Query1Result> {

    @Override
    public Query1Accumulator createAccumulator() {
        return new Query1Accumulator();
    }

    @Override
    public Query1Accumulator add(ShipData shipData, Query1Accumulator query1Accumulator) {
        query1Accumulator.add(shipData.getShipType(), 1);
        return query1Accumulator;
    }

    @Override
    public Query1Accumulator merge(Query1Accumulator acc1, Query1Accumulator acc2) {
        //acc2.getMap().forEach(acc1::add);
        for (Map.Entry<String, Integer> entry : acc2.getMap().entrySet()) {
            String shipType = entry.getKey();
            Integer countAcc2 = entry.getValue();
            if (countAcc2 == null)
                countAcc2 = 0;
            //todo potrebbe essere nullo
            Integer countAcc1 = acc1.getMap().get(shipType);
            if (countAcc1 == null)
                countAcc1 = 0;
            acc1.add(shipType, countAcc1+countAcc2);
        }
        return acc1;
    }

    @Override
    public Query1Result getResult(Query1Accumulator accumulator) {
        return new Query1Result(accumulator.getMap());
    }

}
