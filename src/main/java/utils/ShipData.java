package utils;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class ShipData {
    String tripId;
    String shipId;
    double lat;
    double lon;
    long timestamp;
    String cell;
    String shipType;
    long dateAsTimestamp;
    String sea;

    public final static double lonSeparation = 11.797697;
    private static final double minLat = 32.0;
    private static final double maxLat = 45.0;
    private static final int stepsLat = 10;
    private static final double minLon = -6.0;
    private static final double maxLon = 37.0;
    private static final int stepsLon = 40;

    public ShipData(String shipId, int shipType, double lon, double lat, long timestamp, String tripId) {
        this.tripId = tripId;
        this.shipId = shipId;
        this.lon = lon;
        this.lat = lat;
        this.timestamp = timestamp;
        this.shipType = shipType(shipType);
        this.cell = evaluateCell(lat, lon);
        this.sea = evaluateSea(lon);
    }
    /*public ShipData(String tripId, String shipId, double lon, double lat, long timestamp, int shipType, long dateAsTimestamp) {
        this.tripId = tripId;
        this.shipId = shipId;
        this.lon = lon;
        this.lat = lat;
        this.timestamp = timestamp;
        this.shipType = shipType(shipType);
        this.cell = evaluateCell(lat, lon);
        this.dateAsTimestamp = dateAsTimestamp;

    }*/


    private String evaluateSea(double lon) {

        if (lon<getLonSeparation()){
            return "WEST";
        } else {
            return "EST";
        }
    }


    private String evaluateCell(double lat, double lon){
        char latId = 'A';
        int positionLat = (int)((lat-minLat)/((maxLat-minLat)/stepsLat));
        if (positionLat == 10)
            positionLat--;
        latId += positionLat;

        int lonId = 1;
        int positionLon = (int)((lon-minLon)/((maxLon-minLon)/stepsLon));
        if (positionLon == 40)
            positionLon--;
        lonId += positionLon;

        return ""+latId + lonId;
    }

    private String shipType(int shipType){
        if(shipType == 35){
            return String.valueOf(shipType);
        } else if (shipType >= 60 && shipType <= 69){
            return "60-69";
        } else if (shipType >= 70 && shipType <= 79){
            return "70-79";
        } else {
            return "others";
        }
    }

    /*public static void main(String[] args) {
        ShipData shipData = new ShipData("a", "s", -6, 32, 4, "45");
        System.out.println(shipData.sea);
    }*/

    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }

    public String getShipId() {
        return shipId;
    }

    public void setShipId(String shipId) {
        this.shipId = shipId;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getCell() {
        return cell;
    }

    public void setCell(String cell) {
        this.cell = cell;
    }

    public String getShipType() {
        return shipType;
    }

    public void setShipType(String shipType) {
        this.shipType = shipType;
    }

    public long getDateAsTimestamp() {
        return dateAsTimestamp;
    }

    public void setDateAsTimestamp(long dateAsTimestamp) {
        this.dateAsTimestamp = dateAsTimestamp;
    }

    public static double getLonSeparation() {
        return lonSeparation;
    }

    public static double getMinLat() {
        return minLat;
    }

    public static double getMaxLat() {
        return maxLat;
    }

    public static int getStepsLat() {
        return stepsLat;
    }

    public static double getMinLon() {
        return minLon;
    }

    public static double getMaxLon() {
        return maxLon;
    }

    public static int getStepsLon() {
        return stepsLon;
    }

    public String getSea() {
        return sea;
    }

    public void setSea(String sea) {
        this.sea = sea;
    }
}
