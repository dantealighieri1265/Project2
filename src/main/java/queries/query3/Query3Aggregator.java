package queries.query3;

import org.apache.flink.api.common.functions.AggregateFunction;
import queries.query2.Query2Accumulator;
import queries.query2.Query2Result;
import utils.ShipData;

public class Query3Aggregator implements AggregateFunction<ShipData, Query3Accumulator, Query3Result> {

    @Override
    public Query3Accumulator createAccumulator() {
        return new Query3Accumulator();
    }

    @Override
    public Query3Accumulator add(ShipData shipData, Query3Accumulator query3Accumulator) {
        query3Accumulator.add(shipData.getLon(), shipData.getLat());//aggiornamento dell'accumulator
        return query3Accumulator;
    }

    @Override
    public Query3Result getResult(Query3Accumulator query3Accumulator) {
        return new Query3Result(query3Accumulator);//restituzione dei risultati
    }

    //todo capire se veramente gli accumulatori passati alla merge non verranno più utilizzati in seguito
    @Override
    public Query3Accumulator merge(Query3Accumulator query3Accumulator, Query3Accumulator acc1) {
        query3Accumulator.merge(acc1);//merge degli accumulator
        return query3Accumulator;
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
