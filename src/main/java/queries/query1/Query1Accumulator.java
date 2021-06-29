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


    public Map<String, Integer> getMap() {
        return map;
    }

    public void setMap(Map<String, Integer> map) {
        this.map = map;
    }
}
