package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	
	private List<TDItem> mlistTDItems;
	
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return mlistTDItems.iterator();
    }

    public TupleDesc(List<TDItem> llistTDItems) {
    	mlistTDItems = llistTDItems;
    }
    
    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	
    	mlistTDItems = new ArrayList<TDItem>();
    	
        for(int i=0; i<typeAr.length; i++)
        {
        	TDItem lItem = new TDItem(typeAr[i], fieldAr[i]);
        	mlistTDItems.add(lItem);		
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	mlistTDItems = new ArrayList<TDItem>();
    	
        for(int i=0; i<typeAr.length; i++)
        {
        	TDItem lItem = new TDItem(typeAr[i], null);
        	mlistTDItems.add(lItem);		
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        
        return mlistTDItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        
    	if(i >= mlistTDItems.size())
    		throw new NoSuchElementException("TupleDesc::getFieldName: no field exists at index'"+i+"'.");
    	
        return ((TDItem)mlistTDItems.get(i)).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        
    	if(i >= mlistTDItems.size())
    		throw new NoSuchElementException("TupleDesc::getFieldName: no field exists at index'"+i+"'.");
    	
        return ((TDItem)mlistTDItems.get(i)).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        
    	for(int i=0; i<mlistTDItems.size(); i++)
    	{
    		String lstrFieldName = ((TDItem)mlistTDItems.get(i)).fieldName;
    		if(lstrFieldName != null && lstrFieldName.equals(name))
    			return i;
    	}
    	
    	throw new NoSuchElementException("TupleDesc::fieldNameToIndex: no field exists with name'"+name+"'.");
    	
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        
    	int size = 0;
    	
    	Iterator<TDItem> lIterator = iterator();
    	
    	while(lIterator.hasNext())
    	{
    		size = size + lIterator.next().fieldType.getLen();
    	}
    	
    	return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        
    	Iterator<TDItem> l1Iterator = td1.iterator();
    	Iterator<TDItem> l2Iterator = td2.iterator();
    	
    	List<TDItem> llistTDItems = new ArrayList<>();
    	
    	while(l1Iterator.hasNext())
    		llistTDItems.add((TDItem)l1Iterator.next());
    	
    	while(l2Iterator.hasNext())
    		llistTDItems.add((TDItem)l2Iterator.next());
    	
    	return new TupleDesc(llistTDItems);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        
    	if(o instanceof TupleDesc == false)
    		return false;
    	
    	TupleDesc td = (TupleDesc)o;
    	
    	if(numFields() != td.numFields())
    		return false;
    	
    	Iterator<TDItem> l1Iterator = iterator();
    	Iterator<TDItem> l2Iterator = td.iterator();
    	
    	while(l1Iterator.hasNext())
    	{
    		TDItem l1Item = l1Iterator.next();
    		TDItem l2Item = l2Iterator.next();
    		
    		if(l1Item.toString().equals(l2Item.toString()) == false)
    			return false;
    	}
    	
    	return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	
    	StringBuffer lBuffer = new StringBuffer();
    	
    	for(int i=0; i<mlistTDItems.size(); i++)
    	{
    		lBuffer.append(mlistTDItems.get(i).toString());
    		
    		if(i+1 != mlistTDItems.size())
    			lBuffer.append(",");
    	}
    	
		return lBuffer.toString();
    }
}
