package com.wolfit.selector_threads;

import java.util.Comparator;

public interface RawComparator<T> extends Comparator<T> {
	
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2);
}
