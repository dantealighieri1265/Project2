package queries.query1;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import utils.ShipData;

import java.util.Timer;

public class Query1Trigger extends Trigger<ShipData, TimeWindow> {
    Timer timer = new Timer();
    @Override
    public TriggerResult onElement(ShipData shipData, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        /*System.out.println("ts: "+shipData.getTimestamp());
        System.out.println(timeWindow.getStart()+", "+timeWindow.getEnd()+", "+timeWindow.maxTimestamp());*/
        //onEventTime(shipData.getTimestamp(), timeWindow, triggerContext);
        System.out.println(triggerContext.getCurrentProcessingTime()+", "+triggerContext.getCurrentWatermark());

        if (l == 1432055580000L ){
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;

    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        /*if (l > timeWindow.maxTimestamp()){
            return TriggerResult.FIRE;
        }*/
        //System.out.println(l+", "+timeWindow.getStart()+", "+timeWindow.getEnd()+", "+timeWindow.maxTimestamp());
        return TriggerResult.FIRE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

    }
}
