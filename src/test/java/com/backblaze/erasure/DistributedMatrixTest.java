/**
 * Unit tests for DistributedMatrix
 *
 * Author: Albin Severinson, 2016.
 */

package com.backblaze.erasure;

import org.junit.Test;

//import static org.junit.Assert.assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class DistributedMatrixTest {

    @Test
    public void testMultiply1() {
        Matrix m1 = new Matrix(
                new byte [] [] {
                        new byte [] { 1, 2 },
                        new byte [] { 3, 4 }
                });

        DistributedMatrix m = new DistributedMatrix(m1, 2);
            
        Matrix m2 = new Matrix(
                new byte [] [] {
                        new byte [] { 5, 6 },
                        new byte [] { 7, 8 }
                });
        Matrix actual = m.times(m2);
        // correct answer from java_tables.py
        assertEquals("[[11, 22], [19, 42]]", actual.toString());
    }

    @Test
    public void testMultiply2() {
        Matrix m1 = new Matrix(
                new byte [] [] {
                        new byte [] { 1, 2 },
                        new byte [] { 3, 4 },
                        new byte [] { 1, 2 },
                        new byte [] { 3, 4 }                        
                });

        DistributedMatrix m = new DistributedMatrix(m1, 2);
            
        Matrix m2 = new Matrix(
                new byte [] [] {
                        new byte [] { 5, 6 },
                        new byte [] { 7, 8 }
                });
        Matrix actual = m.times(m2);
        // correct answer from java_tables.py
        assertEquals("[[11, 22], [19, 42], [11, 22], [19, 42]]", actual.toString());
    }    


    @Test
    public void testAddParity() {
        Matrix m = new Matrix(
                new byte [] [] {
                        new byte [] { 1, 2 },
                        new byte [] { 3, 4 }
                });
        DistributedMatrix dm = new DistributedMatrix(m, 2);
        dm.addparity(1);
    }

    @Test
    public void testParityMultiply1() {
        Matrix m1 = new Matrix(
                new byte [] [] {
                        new byte [] { 1, 2 },
                        new byte [] { 3, 4 },
                        new byte [] { 1, 2 },
                        new byte [] { 3, 4 }
                });

        DistributedMatrix m = new DistributedMatrix(m1, 4);
            
        Matrix m2 = new Matrix(
                new byte [] [] {
                        new byte [] { 5, 6 },
                        new byte [] { 7, 8 }
                });

        m.addparity(1);
        
        Matrix actual = m.times(m2);
        // correct answer from java_tables.py
        assertEquals("[[11, 22], [19, 42], [11, 22], [19, 42]]", actual.toString());        
    }

    @Test
    public void testParityMultiply2() {
        Matrix m1 = new Matrix(1, 1).identity(1000);
        DistributedMatrix m = new DistributedMatrix(m1, 10);
        Matrix m2 = new Matrix(1, 1).identity(1000);

        m.addparity(3);
        
        Matrix actual = m.times(m2);

        assert(m1.equals(actual));
    }    
}
