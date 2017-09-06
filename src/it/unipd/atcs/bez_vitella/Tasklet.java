package it.unipd.atcs.bez_vitella;

public abstract class Tasklet {

    //  Master object keeps set of Tasklets
    //  Tasklet is removed from its master when it completes
    //  If Master is empty at that point, notify() is called
    //  master() should only be manipulated in Scheduler or in one
    //  of its Server threads, and only in a synchronized block.

    public Tasklet() {}

    public abstract void run();
}
