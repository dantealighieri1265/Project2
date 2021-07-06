package queries.query2;

import java.io.Serializable;
import java.time.*;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Query2Accumulator implements Serializable {

    private List<String> shipIdsWestAM;
    private List<String> shipIdsWestPM;
    private List<String> shipIdsEstAM;
    private List<String> shipIdsEstPM;

    public Query2Accumulator(){
        this.shipIdsWestAM = new ArrayList<>();
        this.shipIdsWestPM = new ArrayList<>();
        this.shipIdsEstAM = new ArrayList<>();
        this.shipIdsEstPM = new ArrayList<>();
    }
    //todo POTREBBE DIMINUIRE LA LATENZA E THROUGHPUT
    public static void main(String[] args) {
        LocalDateTime localDateTime = Instant.ofEpochMilli(1432055580000L).atZone(ZoneId.systemDefault()).toLocalDateTime();
        LocalDateTime noon = LocalDateTime.of(localDateTime.toLocalDate(), LocalTime.NOON);
        /*System.out.println(localDateTime);
        System.out.println(noon);*/
    }

    public void add(String shipId, String sea, long date){
        LocalDateTime localDateTime = Instant.ofEpochMilli(date).atZone(ZoneId.systemDefault()).toLocalDateTime();
        LocalDateTime noon = LocalDateTime.of(localDateTime.toLocalDate(), LocalTime.NOON);
        LocalDateTime midnight = LocalDateTime.of(localDateTime.toLocalDate(), LocalTime.MIDNIGHT);


        if (sea.equals("WEST")){
            if ((localDateTime.isEqual(midnight) || localDateTime.isAfter(midnight)) && localDateTime.isBefore(noon)){
                if (!shipIdsWestAM.contains(shipId)){
                    shipIdsWestAM.add(shipId);
                }
            }else {
                if (!shipIdsWestPM.contains(shipId)) {
                    shipIdsWestPM.add(shipId);
                }
            }
        } else {
            if ((localDateTime.isEqual(midnight) || localDateTime.isAfter(midnight)) && localDateTime.isBefore(noon)){
                if (!shipIdsEstAM.contains(shipId)){
                    shipIdsEstAM.add(shipId);
                }
            }else {
                if (!shipIdsEstPM.contains(shipId)) {
                    shipIdsEstPM.add(shipId);
                }
            }
        }

    }

    public void merge(Query2Accumulator acc){
        for (String val: acc.getShipIdsWestAM()){
            if (!this.shipIdsWestAM.contains(val)){
                this.shipIdsWestAM.add(val);
            }
        }

        for (String val: acc.getShipIdsWestPM()){
            if (!this.shipIdsWestPM.contains(val)){
                this.shipIdsWestPM.add(val);
            }
        }

        for (String val: acc.getShipIdsEstAM()){
            if (!this.shipIdsEstAM.contains(val)){
                this.shipIdsEstAM.add(val);
            }
        }

        for (String val: acc.getShipIdsEstPM()){
            if (!this.shipIdsEstPM.contains(val)){
                this.shipIdsEstPM.add(val);
            }
        }

    }

    public List<String> getShipIdsWestAM() {
        return shipIdsWestAM;
    }

    public void setShipIdsWestAM(List<String> shipIdsWestAM) {
        this.shipIdsWestAM = shipIdsWestAM;
    }

    public List<String> getShipIdsWestPM() {
        return shipIdsWestPM;
    }

    public void setShipIdsWestPM(List<String> shipIdsWestPM) {
        this.shipIdsWestPM = shipIdsWestPM;
    }

    public List<String> getShipIdsEstAM() {
        return shipIdsEstAM;
    }

    public void setShipIdsEstAM(List<String> shipIdsEstAM) {
        this.shipIdsEstAM = shipIdsEstAM;
    }

    public List<String> getShipIdsEstPM() {
        return shipIdsEstPM;
    }

    public void setShipIdsEstPM(List<String> shipIdsEstPM) {
        this.shipIdsEstPM = shipIdsEstPM;
    }
}
