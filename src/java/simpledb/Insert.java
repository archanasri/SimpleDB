package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId mTId;
    private DbIterator mChild;
    private int miTableId;
    private boolean mbInserted;
    
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        mTId = t;
        mChild = child;
        miTableId = tableId;
        mbInserted = false;
    }

    public TupleDesc getTupleDesc() {
    	return Utility.getTupleDesc(1);
    }

    public void open() throws DbException, TransactionAbortedException {
        mChild.open();
        super.open();
    }

    public void close() {
    	super.close();
    	mChild.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        mChild.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	
    	if(mbInserted == true)
    		return null;
    	
    	int count = 0;
    	
    	while(mChild.hasNext())
    	{
    		Tuple lTuple = mChild.next();
    		try 
    		{
				Database.getBufferPool().insertTuple(mTId, miTableId, lTuple);
			} 
    		catch (IOException e) {
				throw new DbException("Unable to insert tuple.");
			}
    		count++;		
    	}
    	mbInserted = true;
    	return Utility.getTuple(new int[] {count}, 1);
    	
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] {mChild};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        mChild = children[0];
    }
}
