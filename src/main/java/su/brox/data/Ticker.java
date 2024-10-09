package su.brox.data;

public class Ticker {

    private long timestamp;
    private double price;
    private double volume;

    public Ticker(long timestamp, double price, double volume) {
        this.timestamp = timestamp;
        this.price = price;
        this.volume = volume;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getPrice() {
        return price;
    }

    public double getVolume() {
        return volume;
    }
}
