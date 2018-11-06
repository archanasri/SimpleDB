package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId mTId;
    private DbIterator mChild;
    private boolean mbIsDeleted;
    
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
    	mTId = t;
        mChild = child;
        mbIsDeleted = false;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	
    	if(mbIsDeleted == true)
    		return null;
    	
    	int count = 0;
    	
    	while(mChild.hasNext())
    	{
    		Tuple lTuple = mChild.next();
    		try 
    		{
				Database.getBufferPool().deleteTuple(mTId, lTuple);
			} 
    		catch (IOException e) 
    		{
				throw new DbException("Unable to insert tuple.");
			}
    		count++;	
    	}
    	mbIsDeleted = true;
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
