package it.unipd.atcs.bez_vitella;

interface Scheduler {
    public void spawn(Tasklet t);
    //  Add tasklet to t.master and to queue of server t.serverIndex
    public void waitForAll();
    //  Wait for all
    public void printStats();
    //  Shutdown the servers gracefully
    public void shutdown();
}
