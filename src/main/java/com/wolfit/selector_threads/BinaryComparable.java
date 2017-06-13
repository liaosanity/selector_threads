package com.wolfit.selector_threads;

public abstract class BinaryComparable 
    implements Comparable<BinaryComparable> {

    public abstract int getLength();
    
    public abstract byte[] getBytes();
    
    public int compareTo(BinaryComparable other) {
        if (this == other) {
        	return 0;
        }
        
        return WritableComparator.compareBytes(getBytes(), 0, getLength(),
                 other.getBytes(), 0, other.getLength());
    }
    
    public int compareTo(byte[] other, int off, int len) {
        return WritableComparator.compareBytes(getBytes(), 0, getLength(),
                 other, off, len);
    }
    
    public boolean equals(Object other) {
        if (!(other instanceof BinaryComparable)) {
        	return false;
        }
        
        BinaryComparable that = (BinaryComparable)other;
        if (this.getLength() != that.getLength()) {
        	return false;
        }
        
        return this.compareTo(that) == 0;
    }
    
    public int hashCode() {
        return WritableComparator.hashBytes(getBytes(), getLength());
    }
}
