/**
 * Matrix Algebra over an 8-bit Galois Field
 *
 * Copyright 2015, Backblaze, Inc.
 */

package com.backblaze.erasure;

import java.util.Arrays;

/**
 * A matrix partition over the 8-bit Galois field.
 *
 */
public class MatrixPartition {
    private final Matrix matrix;
    private final int order;

    public MatrixPartition(Matrix matrix, int order) {
        this.matrix = matrix;
        this.order = order;
    }
    
    public int getOrder() {
        return order;        
    }

    public Matrix getMatrix() {
        return matrix;
    }

}
