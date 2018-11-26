package simpledb;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private double mNumBuckets, mMin, mMax, mWidth;
    private int miNumTups = 0;
    private TreeMap<Integer,Integer> mHistMap = new TreeMap<Integer, Integer>();

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	mNumBuckets = buckets;
    	mMin = min;
    	mMax = max;
        mWidth = (max - min + 1)/mNumBuckets;
    	int liBucketNum = 0;
    	while(liBucketNum < buckets){
    	    mHistMap.put(liBucketNum, 0);
    	    liBucketNum++;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	double lValueToAdd = v;
    	int liBucketNum = (int)((lValueToAdd - mMin)/mWidth);
    	if(liBucketNum > mNumBuckets - 1 || liBucketNum < 0) {
            return;
        }
       	int liCurrentBucketCount = mHistMap.get(liBucketNum);
    	mHistMap.put(liBucketNum, liCurrentBucketCount + 1);
    	miNumTups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        double lValueToSearch = v - mMin;
        int liRelevantBucket = (int)((lValueToSearch)/mWidth);

        switch(op){
            case EQUALS:
                return selectivityForEquality(liRelevantBucket);
            case NOT_EQUALS:
                return 1 - selectivityForEquality(liRelevantBucket);
            case GREATER_THAN:
                return selectivityForGreaterThan(liRelevantBucket, lValueToSearch);
            case GREATER_THAN_OR_EQ:
                return selectivityForEquality(liRelevantBucket) + selectivityForGreaterThan(liRelevantBucket, lValueToSearch);
            case LESS_THAN:
                return 1 - (selectivityForEquality(liRelevantBucket) + selectivityForGreaterThan(liRelevantBucket, lValueToSearch));
            case LESS_THAN_OR_EQ:
                return 1 - selectivityForGreaterThan(liRelevantBucket, lValueToSearch);
        }
        return 0;
    }

    private double selectivityForEquality(int pRelevantBucket){
        if(mHistMap.get(pRelevantBucket) == null){
            return 0; // No bucket found
        }
        int liHeightOfBucket = mHistMap.get(pRelevantBucket);
        return (liHeightOfBucket / Math.ceil(mWidth)) / miNumTups;
    }

    private double selectivityForGreaterThan(int pRelevantBucket, double v){
        if (pRelevantBucket > mNumBuckets - 1)
            return 0;
        if (pRelevantBucket < 0)
            return 1;
        double lRightMostWidth = (pRelevantBucket + 1)*mWidth;
        double lRelevantWidth = Math.ceil(lRightMostWidth) - v - 1;
        double lSelectivity = selectivityForEquality(pRelevantBucket) * (lRelevantWidth);
        NavigableMap<Integer,Integer> greaterMap = mHistMap.tailMap(pRelevantBucket, false);
        for(Map.Entry<Integer, Integer> entry : greaterMap.entrySet()){
            lSelectivity += selectivityForEquality(entry.getKey());
        }
        return lSelectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}