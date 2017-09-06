package it.unipd.atcs.bez_vitella;

import java.util.Arrays;
import java.util.Random;

public class QuickSort {
    static final boolean Debug = false;
    /*We measure the wall-clock time in nanoseconds using the
     * System.nanoTime() function.
     * We show the time in milliseconds */
    public static final int scale = (int) 1e6;
    /* Compile-Time constants */
    private static final boolean seqEnabled = true;
    private static final boolean parEnabled = true;

    public static double sortArray(
            final Scheduler sched, final int[] arrayToSort, final int seqCutoff) {
        /* Local definition of a the QSortTasklet */
        class QSortTasklet extends Tasklet {

            final int start, end;

            public QSortTasklet(int start, int end) {
                this.start = start;
                this.end = end;
                if (Debug)
                    System.out.println(
                            "Creating tasklet for slice "
                                    + Integer.toString(start)
                                    + " ..< "
                                    + Integer.toString(end));
            }

            public void run() {
                if (Debug)
                    System.out.println(
                            "Starting a tasklet on slice "
                                    + Integer.toString(start)
                                    + " ..< "
                                    + Integer.toString(end));
                if (end - start <= seqCutoff) {
                    Arrays.sort(arrayToSort, start, end);
                } else {
                    int p = quickSort(arrayToSort, start, end - 1);
                    if (start < p + 1) sched.spawn(new QSortTasklet(start, p + 1));
                    if (p + 1 < end) sched.spawn(new QSortTasklet(p + 1, end));
                }
            }
        } // end class QSortTasklet

        QSortTasklet t = new QSortTasklet(0, arrayToSort.length);

        try {
            Thread.sleep(1000); // A little time to set up threads
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        double startTime = System.nanoTime();
        sched.spawn(t);
        sched.waitForAll();
        double endTime = System.nanoTime();
        return (endTime - startTime) / scale;
    } // end sortArray

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: pqsort arr_size num_servers seq_cutoff [seed]");
            return;
        }

        final int arrSize = Integer.parseInt(args[0]);
        final int numServers = Integer.parseInt(args[1]);
        final int seqCutoff = Integer.parseInt(args[2]);
        int seed = 1000; // Default-Value
        if (args.length == 4) {
            seed = Integer.parseInt(args[3]);
        }

        int[] arrayToSort = new int[arrSize];

        Random ran = new Random(seed);

        for (int i = 0; i < arrSize; i++) {
            arrayToSort[i] = ran.nextInt(arrSize * 2);
        }
        if (Debug && arrSize <= 100) {
            for (int i = 0; i < arrSize; i++) {
                System.out.print(Integer.toString(arrayToSort[i]) + " ");
            }
            System.out.println("");
            System.out.println("");
        }

        double seqTime = 0.0;
        if (seqEnabled) {
            int[] seqArrayToSort = Arrays.copyOf(arrayToSort, arrayToSort.length);
            double startTime = System.nanoTime();
            // sequentialQuickSort(seqArrayToSort, 0, seqArrayToSort.length - 1, seqCutoff);
            Arrays.sort(seqArrayToSort);
            double endTime = System.nanoTime();
            seqTime = (endTime - startTime) / scale;

            if (seqEnabled && Debug && seqArrayToSort.length <= 1000) {
                System.out.println("");
                for (int i = 0; i < arrSize; i++) {
                    System.out.print(Integer.toString(seqArrayToSort[i]) + " ");
                }
                System.out.println("");
            }
        }

        WorkStealingScheduler sched = new WorkStealingScheduler(numServers);

        double parTime = 0.0;
        if (parEnabled) parTime = sortArray(sched, arrayToSort, seqCutoff);

        sched.shutdown();

        if (Debug) {
            System.out.println("");
            sched.printStats();
        }

        if (Debug) {
            boolean sorted = true;
            for (int i = 0; i < arrayToSort.length - 1; i++) {
                if (arrayToSort[i] > arrayToSort[i + 1]) {
                    sorted = false;
                    break;
                }
            }
            if (!sorted) {
                throw new Exception("Array not sorted!");
            }

            if (arrSize <= 100) {
                System.out.println("");
                for (int i = 0; i < arrSize; i++) {
                    System.out.print(Integer.toString(arrayToSort[i]) + " ");
                }
                System.out.println("");
            }
        }

        System.out.printf(
                "%d, %d, %d, %d, %f, %f, %d, %d\n",
                arrSize,
                numServers,
                seqCutoff,
                seed,
                seqTime,
                parTime,
                sched.getTotalInits(),
                sched.getTotalSteals());
    }

    private static int quickSort(int array[], int lowerIndex, int higherIndex) {
        int i = lowerIndex;
        int j = higherIndex;
        int pivot = array[i];
        while (true) {
            while (array[i] < pivot) {
                i++;
            }
            while (array[j] > pivot) {
                j--;
            }
            if (i >= j) {
                return j;
            }
            swap(array, i, j);
            i++;
            j--;
        }
    }

    private static void swap(int array[], int i, int j) {
        int temp = array[i];
        array[i] = array[j];
        array[j] = temp;
    }
} // end class QuickSort
