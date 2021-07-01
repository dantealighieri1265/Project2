package queries.query3;

import java.io.Serializable;

import org.apache.lucene.spatial.util.GeoDistanceUtils;

public class Query3Accumulator implements Serializable {

    private double lon;
    private double lat;
    private double distance;


    public Query3Accumulator(){
        this.lon = Double.NaN;
        this.lat = Double.NaN;
        this.distance = 0.0;
    }

    public static void main(String[] args) {

        double l = GeoDistanceUtils.vincentyDistance(0.0,0.0, 50.0,0.0);
        System.out.println(l);
    }

    public void add(double lon, double lat) {
        if(!Double.isNaN(getLon()) && !Double.isNaN(getLat())){
            //todo semplicemente vuole la distanza euclidea forse
            //double linearDistance = GeoDistanceUtils.linearDistance(new double[]{getLon(), getLat()}, new double[]{lon, lat});
            double linearDistance = GeoDistanceUtils.vincentyDistance(getLon(), getLat(), lon, lat);
            setDistance(linearDistance);
        }
        setLon(lon);
        setLat(lat);
    }

    public void merge(Query3Accumulator acc1, Query3Accumulator acc2){
        acc1.setDistance(acc1.getDistance()+acc2.getDistance());
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }
}
