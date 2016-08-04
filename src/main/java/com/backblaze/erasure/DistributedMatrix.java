/**
 * Distributed Matrix Algebra over a 8-bit Galois Field
 *
 * Author Albin Severinson, 2016.
 */

package com.backblaze.erasure;

import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.concurrent.*;

// To simulate delay
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A matrix over the 8-bit Galois field for which operations are
 * performed in a distributed manner.
 *
 * This class is not performance-critical, so the implementations
 * are simple and straightforward.
 */
public class DistributedMatrix {  
    private final int partitionCount;
    private final int partitionHeight;
    private final int columns;
    private final List<Matrix> partitions = new LinkedList<Matrix>();
    private final List<Matrix> parity = new LinkedList<Matrix>();
    //private final ExecutorService executor = Executors.newCachedThreadPool();
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    
    public DistributedMatrix(Matrix matrix, int partitionCount) {
        partitionHeight = matrix.getRows() / partitionCount;        
        if (partitionHeight < 1) {
            throw new IllegalArgumentException("Partition height must be at least 1");
        }
        if (matrix.getRows() % partitionCount != 0) {
            throw new IllegalArgumentException("Number of partitions must be a divisor of the matrix height");
        }

        this.partitionCount = partitionCount;
        this.columns = matrix.getColumns();

        for (int s = 0; s < partitionCount; s++) {
            partitions.add(matrix.submatrix(s * partitionHeight,
                                            0,
                                            (s + 1) * partitionHeight,
                                            matrix.getColumns()));
        }
    }

    /**
     * Calculate parity.
     */
    public void addparity(int parityCount) {
        byte[][] shards = new byte [partitionCount + parityCount] [];

        Iterator<Matrix> iterator = partitions.iterator();
        
        for (int s = 0; s < partitionCount; s++) {
            Matrix matrix = iterator.next();
            shards[s] = matrix.reshape(1, matrix.getRows() * matrix.getColumns()).getRow(0);
        }
        for (int s = partitionCount; s < partitionCount + parityCount; s++) {
            shards[s] = new byte[columns * partitionHeight];
        }

        // Use Reed-Solomon to calculate the parity.
        ReedSolomon reedSolomon = ReedSolomon.create(partitionCount, parityCount);
        reedSolomon.encodeParity(shards, 0, columns * partitionHeight);

        for (int s = partitionCount; s < partitionCount + parityCount; s++) {
            byte[][] data = new byte [1] [];
            data[0] = shards[s];
            Matrix matrix = new Matrix(data);
            parity.add(matrix.reshape(matrix.getColumns() / columns, columns));
        }
    }

    /*
    public void decode(byte[][] shards, boolean[] shardPresent, int parityCount) {
        int partitionHeight = this.getRows() / partitions;
        if (partitionHeight < 1) {
            throw new IllegalArgumentException("Partition height must be at least 1");
        }

        //byte[][] shards = new byte [partitions + parityCount] [this.getColumns * partitionHeight];
        
        
        // Use Reed-Solomon to decode missing shards
        ReedSolomon reedSolomon = ReedSolomon.create(partitions, parityCount);
        reedSolomon.decodeMissing(shards, shardPresent, 0, this.getColumns() * partitionHeight);

        return shards;
    }
    */

    /**
     * Multiplies this matrix (the one on the left) by another
     * matrix (the one on the right).
     */
    public Matrix times(Matrix right) {
        if (columns != right.getRows()) {
            throw new IllegalArgumentException(
                    "Columns on left (" + columns +") " +
                    "is different than rows on right (" + right.getRows() + ")");
        }

        int DATA_SHARDS = partitions.size();
        int PARITY_SHARDS = parity.size();

        // Create a list for the workers to submit their work
        List<MatrixPartition> results = new LinkedList<MatrixPartition>();

        // Create a countdown latch no notify the main thread of then
        // the results are in.
        CountDownLatch latch = new CountDownLatch(DATA_SHARDS);

        // Store futures for the workers for use in canceling
        // remaining workers.
        Future[] futures = new Future[DATA_SHARDS + PARITY_SHARDS];

        // Submit the data partitions to the worker pool
        int order = 0;
        Iterator<Matrix> iterator = partitions.iterator();
        while (iterator.hasNext()){
            Matrix left = iterator.next();
            futures[order] = executor.submit(new TimesWorker(order++, left, right, results, latch));
        }

        // Submit the parity partitions to the worker pool
        iterator = parity.iterator();
        while (iterator.hasNext()){
            Matrix left = iterator.next();
            futures[order] = executor.submit(new TimesWorker(order++, left, right, results, latch));
        }

        // Wait for results to come in...
        System.out.println("waiting for results to come in.");
        try {
            latch.await();
        } catch(InterruptedException e) {
            System.out.println("Interrupted while waiting for latch.");
        }
        System.out.println("Result size=" + results.size());

        // Stop the workers still running
        for (int i = 0; i < DATA_SHARDS; i++) {
            futures[i].cancel(true);
        }

        int rowCount = DATA_SHARDS + PARITY_SHARDS;
        int resultColumns = 0;
        byte[][] shards = new byte [rowCount] [];
        Iterator<MatrixPartition> partitionIterator = results.iterator();
        while (partitionIterator.hasNext()) {
            MatrixPartition partition = partitionIterator.next();
            //int order = partition.getOrder();
            Matrix result = partition.getMatrix();
            System.out.println("result=" + result);
            shards[partition.getOrder()] = result.reshape(1, result.getRows() * result.getColumns()).getRow(0);
            System.out.println("rows=" + result.getRows() + " columns=" + result.getColumns());
            resultColumns = result.getRows() * result.getColumns(); //TODO: cleanup
        }

        // Store which shards are missing and allocate byte arrays for
        // those slots.
        boolean[] shardPresent = new boolean[DATA_SHARDS + PARITY_SHARDS];               
        for (int i = 0; i < rowCount; i++) {
            if (shards[i] == null) {
                shards[i] = new byte[resultColumns];
            } else {
                shardPresent[i] = true;
            }
        }

        System.out.println("shardPresent:");
        for (int i = 0; i < DATA_SHARDS + PARITY_SHARDS; i++) {
            System.out.print(i + "=" + shardPresent[i] + " ");
        }
        System.out.println("");

        System.out.println("resultColumns=" + resultColumns);
        System.out.println("before decoding=" + new Matrix(shards));
        
        // Use Reed-Solomon to decode missing shards
        ReedSolomon reedSolomon = ReedSolomon.create(DATA_SHARDS, PARITY_SHARDS);
        reedSolomon.decodeMissing(shards, shardPresent, 0, resultColumns);

        Matrix decoded = new Matrix(shards);
        System.out.println("decoded=" + decoded);

        Matrix result = null;
        for (int r = 0; r < DATA_SHARDS; r++) {
            Matrix submatrix = decoded.submatrix(r, 0, r+1, decoded.getColumns());
            if (result == null) {
                result = submatrix.reshape(submatrix.getColumns() / right.getColumns(), right.getColumns());
            } else {
                result = result.concatenate(submatrix.
                                            reshape(submatrix.getColumns() / right.getColumns(),
                                                    right.getColumns()));
            }
        }       

        return result;
    }

    private class TimesWorker implements Runnable {
        private final int order;
        private final Matrix left;
        private final Matrix right;
        private final List<MatrixPartition> results;
        private final CountDownLatch latch;

        Random random = new Random();
        
        public TimesWorker(int order, Matrix left, Matrix right,
                           List<MatrixPartition> results, CountDownLatch latch) {
            this.order = order;
            this.left = left;
            this.right = right;
            this.results = results;
            this.latch = latch;
        }

        public void run() {
            // Simulate delay
            try {
                System.out.println("Worker " + order + " sleeping.");
                Thread.sleep(random.nextInt(32));
                System.out.println("Worker " + order + " waking up.");                
            } catch(InterruptedException e) {
                System.out.println("Order " + order + " thread canceled.");
                return;
            }

            System.out.println("Worker " + order + " starting.");
            Matrix result = left.times(right);
            results.add(new MatrixPartition(result, order));
            latch.countDown();
            System.out.println("Order " + order + " thread finished.");
            return;
        }
    }
}
