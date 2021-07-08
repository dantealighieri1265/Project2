package queries.ktm;

import org.apache.flink.api.java.tuple.Tuple2;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

public class Query2Result {

    private LocalDateTime startDate;
    private LocalDateTime endDate;
    private String sea;
    //dimensioni delle rispettive liste
    private HashMap<Tuple2<String, String>, Long> mapAM;
    private HashMap<Tuple2<String, String>, Long> mapPM;
    private TreeMap<Long, List<String>> treeMapAM = new TreeMap<>(Collections.reverseOrder());
    private TreeMap<Long, List<String>> treeMapPM = new TreeMap<>(Collections.reverseOrder());



    public Query2Result(Query2Accumulator query2Accumulator) {
        this.mapAM = query2Accumulator.getMapAM();
        this.mapPM = query2Accumulator.getMapPM();
    }

    public LocalDateTime getEndDate() {
        return endDate;
    }

    public void setEndDate(LocalDateTime endDate) {
        this.endDate = endDate;
    }

    public LocalDateTime getStartDate() {
        return startDate;
    }

    public void setStartDate(LocalDateTime startDate) {
        this.startDate = startDate;
    }

    public String getSea() {
        return sea;
    }

    public void setSea(String sea) {
        this.sea = sea;
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

    public TreeMap<Long, List<String>> getTreeMapAM() {
        return treeMapAM;
    }

    public void setTreeMapAM(TreeMap<Long, List<String>> treeMapAM) {
        this.treeMapAM = treeMapAM;
    }

    public TreeMap<Long, List<String>> getTreeMapPM() {
        return treeMapPM;
    }

    public void setTreeMapPM(TreeMap<Long, List<String>> treeMapPM) {
        this.treeMapPM = treeMapPM;
    }

    @Override
    public String toString() {
        return "Query2Result{" +
                "startDate=" + startDate +
                ", endDate=" + endDate +
                ", sea='" + sea + '\'' +
                ", mapAM=" + mapAM +
                ", mapPM=" + mapPM +
                ", treeMapAM=" + treeMapAM +
                ", treeMapPM=" + treeMapPM +
                '}';
    }
}
