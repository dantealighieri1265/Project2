package queries.query1;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Query1Accumulator implements Serializable {

    //mappa (tiponave - count)
    private Map<String, Integer> map;


    public Query1Accumulator(){
        this.map = new HashMap<>();
        this.map.put("35", 0);
        this.map.put("60-69", 0);
        this.map.put("70-79", 0);
        this.map.put("others", 0);
    }

    public Query1Accumulator(Map<String, Integer> map) {
        this.map = map;
    }

    /*public void add(String shipType, Set<String> tripsSet){
        for (String tripId : tripsSet) {
            add(shipType, tripId);
        }
    }*/

    public void add(String shipType, Integer value){
        Integer count = map.get(shipType);
        if(count == null){
            count = value;
        }else{
            count++;//update value
        }
        map.put(shipType, count);
    }

    public Query1Accumulator merge(Query1Accumulator acc1, Query1Accumulator acc2){
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

    public Map<String, Integer> getMap() {
        return map;
    }

    public void setMap(Map<String, Integer> map) {
        this.map = map;
    }
}
