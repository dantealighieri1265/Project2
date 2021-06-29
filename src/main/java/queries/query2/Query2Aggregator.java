package queries.query2;

import org.apache.flink.api.common.functions.AggregateFunction;
import utils.ShipData;

public class Query2Aggregator implements AggregateFunction<ShipData, Query2Accumulator, Query2Result> {

    @Override
    public Query2Accumulator createAccumulator() {
        return new Query2Accumulator();
    }

    @Override
    public Query2Accumulator add(ShipData shipData, Query2Accumulator query2Accumulator) {
        query2Accumulator.add(shipData.getShipId(), shipData.getSea(), shipData.getTimestamp());
        return query2Accumulator;
    }

    @Override
    public Query2Result getResult(Query2Accumulator query2Accumulator) {
        return new Query2Result(query2Accumulator);
    }

    @Override
    public Query2Accumulator merge(Query2Accumulator query2Accumulator, Query2Accumulator acc2) {
        query2Accumulator.merge(acc2);
        return query2Accumulator;
    }

    /*@Override
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
    public Query1Outcome getResult(Query1Accumulator accumulator) {
        return new Query1Outcome(accumulator.getMap());
    }*/

}
