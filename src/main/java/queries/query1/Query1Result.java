package queries.query1;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

public class Query1Result {

    private Map<String, Integer> map;

    public LocalDateTime getEndDate() {
        return endDate;
    }

    public void setEndDate(LocalDateTime endDate) {
        this.endDate = endDate;
    }

    private LocalDateTime startDate;
    private LocalDateTime endDate;
    private String cellId;

    public Query1Result() {
    }

    /*public Query1Outcome(Map<String, Set<String>> typeMapInput){
        this.map = new HashMap<>();
        for(String shipType: typeMapInput.keySet()){
            this.map.put(shipType, typeMapInput.get(shipType).size());
        }
    }*/

    public Query1Result(Map<String, Integer> map){
        this.map = map;
    }

    public Query1Result(Map<String, Integer> map, LocalDateTime startDate) {
        this.map = map;
        this.startDate = startDate;
    }

    public Map<String, Integer> getMap() {
        return map;
    }

    public void setMap(Map<String, Integer> map) {
        this.map = map;
    }

    public LocalDateTime getStartDate() {
        return startDate;
    }

    public void setStartDate(LocalDateTime startDate) {
        this.startDate = startDate;
    }

    public String getCellId() {
        return cellId;
    }

    public void setCellId(String cellId) {
        this.cellId = cellId;
    }

    @Override
    public String toString() {
        return "Query1Outcome{" +
                "typeMap=" + map +
                ", date=" + startDate +
                ", cellId='" + cellId + '\'' +
                '}';
    }
}
