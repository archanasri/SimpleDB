package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int miGBField;
    private Type mGBFieldType;
    private int miAggField;
    private Op mOp;
    
    /**
     * Stores the description of the tuple: One field if no grouping, two fields if grouping.
     */
    private TupleDesc mTupleDesc;
    
    /**
     * Result of Aggregations are stores in this list
     */
    private List<Tuple> mAggregateTuples = new ArrayList<Tuple>();
    
    /**
     * To find average of values across each field, we need to store the count of occurrence of that field.<BR>
     * If no grouping, we store count against IntField(-1).
     */
    private Map<Field, Integer> mCountMap = new HashMap<Field, Integer>();
    
    /**
     * To find average of values across each field, we need to store the sum of occurrences of that field.<BR>
     * If no grouping, we store count against IntField(-1).
     */
    private Map<Field, Integer> mSumMap = new HashMap<Field, Integer>();
    
    
    /**	
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	miGBField = gbfield;
        mGBFieldType = gbfieldtype;
        miAggField = afield;
        mOp = what;
        
        if(miGBField == Aggregator.NO_GROUPING)
        {
        	Type[] types = new Type[] {Type.INT_TYPE};
        	mTupleDesc = new TupleDesc(types);
        }
        else
        {
        	Type[] types = new Type[] {mGBFieldType, Type.INT_TYPE};
        	mTupleDesc = new TupleDesc(types);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param pTuple - the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple pTuple) {
        
    	// If there is no grouping, then the iterator will contain only one tuple with the result.
    	// Else the iterator will contain one tuple per group.
    	
    	Iterator<Tuple> it = mAggregateTuples.iterator();

        while (it.hasNext()) 
        {
            Tuple lTuple = it.next();
            
            if(miGBField == Aggregator.NO_GROUPING) 
            {
                int val = ((IntField)lTuple.getField(0)).getValue();
                IntField newField = new IntField(getAggregatedValue(val, pTuple));
                lTuple.setField(0, newField);
                return;
            }
            
            Field f1 = lTuple.getField(0);
            Field f2 = pTuple.getField(miGBField);
            
            if (f1.equals(f2)) {
                int val = ((IntField) lTuple.getField(1)).getValue();
                IntField newField = new IntField(getAggregatedValue(val, pTuple));
                lTuple.setField(1, newField);
                return;
            }
        }

        // Cases when we reach this point.
        // No grouping: First tuple we saw
        // Grouping: First time we saw this Group By Field
        
        Tuple lTuple = new Tuple(mTupleDesc);
        
        //Setting initial value of the Aggregate Value
        Field lInitialAggField = null; 
        if(mOp == Op.COUNT)
        	lInitialAggField = new IntField(1);
        else
        	lInitialAggField = pTuple.getField(miAggField);
    	
        //Setting 
        Field lGroupField = null;
        if (miGBField == Aggregator.NO_GROUPING)
        {
        	//Default Field to be used with mCountMap and mSumMap for the Average Operation
        	lGroupField = new IntField(Aggregator.NO_GROUPING);
        	lTuple.setField(0, lInitialAggField);
        }
        else
        {
        	lGroupField = pTuple.getField(miGBField);
            lTuple.setField(0, lGroupField);
            lTuple.setField(1, lInitialAggField);
        }

        mAggregateTuples.add(lTuple);
        
        mCountMap.put(lGroupField, 1);
        mSumMap.put(lGroupField, ((IntField)pTuple.getField(miAggField)).getValue());
    	
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        return new TupleIterator(mTupleDesc, mAggregateTuples);
    }
    
    private int getAggregatedValue(int existingValue, Tuple pTuple)
    {
        int value = ((IntField) pTuple.getField(miAggField)).getValue();
        
        switch(mOp) 
        {
            case COUNT:
                return existingValue + 1;
                
            case MIN:
                return existingValue < value ? existingValue : value;
                
            case MAX:
                return existingValue > value ? existingValue : value;
                
            case SUM:
                return existingValue + value;
                
            case AVG:
                
            	Field mapField = null;
                
                if (miGBField != Aggregator.NO_GROUPING)
                    mapField = pTuple.getField(miGBField);
                else
                    mapField = new IntField(Aggregator.NO_GROUPING);
                
                //GetAggregatedValue is called for an existing group. So CountMap and SumMap will always have values for the mapField.
                
                int curCount = mCountMap.get(mapField) + 1;
                int curSum = mSumMap.get(mapField) + value;
                
                mCountMap.put(mapField, curCount);
                mSumMap.put(mapField, curSum);
                
                return curSum/curCount;
            default:
            	return -1;
        }
        
    }

}
