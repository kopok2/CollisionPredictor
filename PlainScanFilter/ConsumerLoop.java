package tech.oleszek;

public interface ConsumerLoop extends Runnable {
    void run();
    void shutdown();
}