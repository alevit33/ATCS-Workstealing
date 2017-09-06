package it.unipd.atcs.bez_vitella;

import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkStealingScheduler implements Scheduler {
    private static boolean Debug = false;
    private static AtomicBoolean shutdownNow = new AtomicBoolean(false);
    private static ThreadLocal<Integer> serverIndex = new ThreadLocal<Integer>();
    private final ServerThread[] servers;
    public final HashSet<Tasklet> master = new HashSet<Tasklet>();
    private Integer totalSteals = null;
    private Integer totalInits = null;

    public void shutdown() {
        shutdownNow.set(true);
        if (Debug) System.out.println("Shutdown started...");
    }

    class Statistics {
        public int numTaskletInitiations = 0;
        public int numTaskletSteals = 0;
    }

    private class ServerThread extends Thread {
        private int myIndex;
        public ConcurrentLinkedDeque<Tasklet> deque = new ConcurrentLinkedDeque<Tasklet>();
        public final Statistics stats = new Statistics();
        // We can compile a version without workstealing, so that we can compare
        // the results in the benchmark
        private static final boolean stealingEnabled = true;

        public void run() { // service own queue of Tasklets
            if (Debug) System.out.println("Server " + myIndex + " started");
            serverIndex.set(myIndex); // set thread-local storage value
            while (!shutdownNow.get()) {
                Tasklet t;
                t = deque.pollFirst();
                if (t != null) { // There is something
                    t.run();
                    removeFromMaster(t);
                } else if (stealingEnabled) {
                    int newIndex;
                    for (int i = 1; i < servers.length; i++) {
                        newIndex = (myIndex + i) % servers.length;
                        t = servers[newIndex].deque.pollLast();
                        if (t != null) {
                            stats.numTaskletSteals++;
                            t.run();
                            removeFromMaster(t);
                            break;
                        }
                    }
                }
            }
        } // end run

        public ServerThread(int myIndex) {
            this.myIndex = myIndex;
        }
    } // end class ServerThread

    public void addToMaster(Tasklet t) {
        synchronized (master) {
            master.add(t);
        }
    }

    public void removeFromMaster(Tasklet t) {
        synchronized (master) {
            master.remove(t);
            if (master.isEmpty()) {
                master.notify();
            }
        }
    }

    public WorkStealingScheduler(int numServers) {
        //  Create the array of servers, and then start them all running
        this.servers = new ServerThread[numServers];
        for (int i = 0; i < numServers; i++) {
            servers[i] = new ServerThread(i);
        }
        for (int i = 0; i < numServers; i++) {
            servers[i].start();
        }
    }

    public void spawn(Tasklet t) {
        int myIndex = 0;
        if (serverIndex.get() != null) {
            myIndex = serverIndex.get();
        }
        addToMaster(t);
        servers[myIndex].deque.addFirst(t);
        servers[myIndex].stats.numTaskletInitiations++;
    }

    public void waitForAll() {
        try {
            synchronized (master) {
                while (!master.isEmpty()) {
                    master.wait();
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void computeStats() {
        if (totalSteals != null && totalInits != null) return;

        totalSteals = new Integer(0);
        totalInits = new Integer(0);
        for (int i = 0; i < servers.length; ++i) {
            int numSteals = servers[i].stats.numTaskletSteals;
            int numInitiations = servers[i].stats.numTaskletInitiations;
            totalSteals += numSteals;
            totalInits += numInitiations;
        }
    }

    public int getTotalSteals() {
        computeStats();
        return totalSteals;
    }

    public int getTotalInits() {
        computeStats();
        return totalInits;
    }

    public void printStats() {
        System.out.println("server\tsteals\tinits");
        int totalSteals = 0;
        int totalInitiations = 0;
        for (int i = 0; i < servers.length; i++) {
            int numSteals = servers[i].stats.numTaskletSteals;
            int numInitiations = servers[i].stats.numTaskletInitiations;

            totalSteals += numSteals;
            totalInitiations += numInitiations;
            System.out.println(Integer.toString(i) + "\t" + numSteals + "\t" + numInitiations);
        }
        System.out.println("total\t" + totalSteals + "\t" + totalInitiations);
    }
} // end class WorkStealingScheduler
