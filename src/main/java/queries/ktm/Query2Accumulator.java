package queries.ktm;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.*;

public class Query2Accumulator implements Serializable {

    private HashMap<Tuple2<String, String>, Long> mapAM;
    private HashMap<Tuple2<String, String>, Long> mapPM;

    /**
     * Costruttore: genera le liste da aggiornare
     */
    public Query2Accumulator(){
        mapAM = new HashMap<>();
        mapPM = new HashMap<>();
    }

    /**
     *
     * @param shipId stringa contenente l'id della tratta considerata
     * @param cellId stringa contenente l'id della cella
     * @param date timestamp relativo alla tupla considerata
     */
    public void add(String shipId, String cellId, long date){
        LocalDateTime localDateTime = Instant.ofEpochMilli(date).atZone(ZoneId.systemDefault()).toLocalDateTime();
        LocalDateTime noon = LocalDateTime.of(localDateTime.toLocalDate(), LocalTime.NOON);
        LocalDateTime midnight = LocalDateTime.of(localDateTime.toLocalDate(), LocalTime.MIDNIGHT);

        //verifica l'orario in cui la nave attraversa la cella del Mar Occidentale
        if ((localDateTime.isEqual(midnight) || localDateTime.isAfter(midnight)) && localDateTime.isBefore(noon)){
            mapAM.put(new Tuple2<>(cellId, shipId), 1L);
        }else {
            mapPM.put(new Tuple2<>(cellId, shipId), 1L);
        }

    }

    /**
     * @param acc accumulatore da unire ad acc1
     */
    public void merge(Query2Accumulator acc){
        //aggiornamento liste
        this.mapAM.putAll(acc.mapAM);
        this.mapPM.putAll(acc.mapPM);

    }

    public HashMap<Tuple2<String, String>, Long> getMapAM() {
        return mapAM;
    }

    public void setMapAM(HashMap<Tuple2<String, String>, Long> mapAM) {
        this.mapAM = mapAM;
    }

    public HashMap<Tuple2<String, String>, Long> getMapPM() {
        return mapPM;
    }

    public void setMapPM(HashMap<Tuple2<String, String>, Long> mapPM) {
        this.mapPM = mapPM;
    }
}
