package queries.query1;

import org.apache.flink.api.common.functions.AggregateFunction;
import utils.ShipData;

public class Query1Aggregator implements AggregateFunction<ShipData, Integer, String> {

    @Override
    public Integer createAccumulator() {
        return 0;
    }

    @Override
    public Integer add(ShipData shipData, Integer integer) {
        return (int) shipData.getLat() + integer;
    }

    @Override
    public String getResult(Integer integer) {
        return integer.toString();
    }

    @Override
    public Integer merge(Integer integer, Integer acc1) {
        return integer+acc1;
    }
}
