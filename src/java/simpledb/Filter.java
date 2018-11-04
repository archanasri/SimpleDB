package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate mPredicate;
    private DbIterator mChild;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
    	mPredicate = p;
    	mChild = child;
    }

    public Predicate getPredicate() {
        // some code goes here
    	return mPredicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	return mChild.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	mChild.open();
    	super.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	mChild.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	mChild.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	while(mChild.hasNext() != false) {
    		Tuple t = mChild.next();
    		if (mPredicate.filter(t) == true) {
    			return t;
    		}
    	}
    	return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	DbIterator[] result = new DbIterator[1];
    	result[0] = mChild;
        return result;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	mChild = children[0];
    }

}
