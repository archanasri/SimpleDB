package simpledb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	private File mFile;
	private TupleDesc mTupleDesc; 
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
    	mFile = f;
    	mTupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return mFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        
    	return mFile.getAbsoluteFile().hashCode();
    	
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return mTupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	
    	HeapPage lPage = null;
    	RandomAccessFile lraFile = null;
    	
    	try 
    	{
    		byte[] lPageData = HeapPage.createEmptyPageData();
			
    		lraFile = new RandomAccessFile(mFile, "r");
			lraFile.seek(pid.pageNumber()*BufferPool.getPageSize());
			lraFile.read(lPageData);
			
			HeapPageId lHPId = new HeapPageId(pid.getTableId(), pid.pageNumber());
			lPage = new HeapPage(lHPId, lPageData);
			
			lraFile.close();
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			try {
				if(lraFile != null)
					lraFile.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		}
		
		return lPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	
    	double ldFileSize = mFile.length();
        double ldPageSize = BufferPool.getPageSize();
        return (int)Math.ceil(ldFileSize/ldPageSize);
    	
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1|lab2
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1|lab2
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        //TODO
    	return new HeapFileIterator(this, tid);
    }
    
    class HeapFileIterator implements DbFileIterator{
    	
    	private HeapFile mHeapFile;
        private TransactionId mTransactionId;
        private Iterator<Tuple> mIterator;
        private int miCurrentPage;

        public HeapFileIterator(HeapFile heapFile, TransactionId tId){
            this.mHeapFile = heapFile;
            mTransactionId = tId;
            mIterator = null;
            miCurrentPage = -1;
        }

    	
    	@Override
    	public void open() throws DbException, TransactionAbortedException {
    		miCurrentPage = 0;
            mIterator = tupleIterator(miCurrentPage);
    	}

    	@Override
    	public boolean hasNext() throws DbException, TransactionAbortedException {
    		if(miCurrentPage == -1)
    		{
                return false;
            } 
    		
    		if(mIterator.hasNext())
    		{
                return true;
            } 
    		
    		while (miCurrentPage < mHeapFile.numPages() - 1) {
    			if(mIterator.hasNext())
                	return true;
    			miCurrentPage += 1;
                mIterator = tupleIterator(miCurrentPage);
            }
            return mIterator.hasNext();
    		
        }

    	@Override
    	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
    		if(hasNext())
    		{
                return mIterator.next();
            } 
    		throw new NoSuchElementException("Reached end of iterator");
    	}

    	@Override
    	public void rewind() throws DbException, TransactionAbortedException {
    		close();
            open();
    	}

    	@Override
    	public void close() {
    		mIterator = null;
    		miCurrentPage = -1;
    	}
    	
    	public Iterator<Tuple> tupleIterator(int pPageNo) throws TransactionAbortedException, DbException {
            PageId lHeapPageId = new HeapPageId(mHeapFile.getId(), pPageNo);
            HeapPage lHeapPage = (HeapPage) Database.getBufferPool().getPage(mTransactionId, lHeapPageId, Permissions.READ_ONLY);
            return lHeapPage.iterator();
        }
    }
    
}

