package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int miNumPages;
    
    private Map<PageId, Page> mmapPages;
    
    private ConcurrencyControl mCControl;
    
    //Using FIFO approach for Eviction Policy
    //private Deque<PageId> mPidQueue;
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
    	miNumPages = numPages;
    	mmapPages = new HashMap<>();
    	//mPidQueue = new LinkedList<>();
    	mCControl = new ConcurrencyControl();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	
    	LockType lType;
        if(perm == Permissions.READ_ONLY)
            lType = LockType.Slock;
        else
            lType = LockType.Xlock;

        mCControl.acquireLock(tid, pid, lType);
    	
    	if(mmapPages.containsKey(pid))
    		return mmapPages.get(pid);
    	
    	if(mmapPages.size() >= miNumPages)
    	{
    		//Insufficient space in Bufferpool
    		//throw new DbException("Insufficient space in Bufferpool");
    		evictPage();
    	}
    	
    	//There is space. Get Page and add to map.
    	Page lPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
    	mmapPages.put(pid, lPage);
    	//mPidQueue.addLast(pid);
    	
        return lPage;
    	    	
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {

    	mCControl.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {

    	transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        
    	return mCControl.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        
    	List<PageId> lLockedPages = mCControl.mTIdPageMap.get(tid);
        if(lLockedPages!=null){
            for(PageId pageId: lLockedPages)
            {
                if(commit)
                {
                    flushPage(pageId);
                } 
                else if(mmapPages.get(pageId) != null && mmapPages.get(pageId).isDirty() != null)
                {
                    discardPage(pageId);
                }
            }
        }
        mCControl.releaseLock(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        
    	DbFile lTable = Database.getCatalog().getDatabaseFile(tableId);
    	List<Page> lPages = lTable.insertTuple(tid, t);
    	
    	for(Page lPage : lPages)
    	{
    		lPage.markDirty(true, tid);
    		mmapPages.put(lPage.getId(), lPage);
    	}
    	
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        
    	DbFile lTable = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
    	List<Page> lPages = lTable.deleteTuple(tid, t);
    	
    	for(Page lPage : lPages)
    	{
    		lPage.markDirty(true, tid);
    		mmapPages.put(lPage.getId(), lPage);
    	}
    	
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {

    	// flushAllPages should call flushPage on all pages in the BufferPool
    	for(PageId pid : mmapPages.keySet())
    	{
    		flushPage(pid);
    	}
    	
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {

    	mmapPages.remove(pid);
    }	

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {

    	// flushPage should write any dirty page to disk and mark it as not dirty, while leaving it in the BufferPool
		//Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(mmapPages.get(pid));
    	
    	if(mmapPages.containsKey(pid))
    	{
            Page lPage = mmapPages.get(pid);
            if(lPage.isDirty()!= null)
            {
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(lPage);
                lPage.markDirty(false, null);
            }
        }
    	
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {

    	if(mCControl.mTIdPageMap.containsKey(tid))
    	{
            ArrayList<PageId> llistPages = mCControl.mTIdPageMap.get(tid);
            for (PageId lPage : llistPages)
                flushPage(lPage);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        
    	// The only method which should remove page from the buffer pool is evictPage, 
    	// which should call flushPage on any dirty page it evicts.
    	
//    	PageId pid = mPidQueue.pollFirst();
//    	
//    	if(pid != null)
//    	{
//    		try 
//    		{
//				flushPage(pid);
//				mmapPages.remove(pid);
//			} 
//    		catch (IOException e) 
//    		{
//				throw new DbException("Error while writing page to disk.");
//			}
//    		
//    	}
    	
    	for (Map.Entry<PageId, Page> lEntry : mmapPages.entrySet()) 
    	{
            PageId lPId = lEntry.getKey();
            Page lPage = lEntry.getValue();
            if (lPage.isDirty() == null) 
            {                
                discardPage(lPId);
                return;
            }
        }
        throw new DbException("BufferPool: evictPage: all pages are marked as dirty");
    	
    }

}

enum LockType{Slock, Xlock}

class LockData
{
    LockType mLockType;
    ArrayList<TransactionId> mTransactions;

    LockData(LockType pLockType, ArrayList<TransactionId> pTId)
    {
        this.mLockType = pLockType;
        this.mTransactions = pTId;
    }
}

class ConcurrencyControl
{
    ConcurrentHashMap<TransactionId, ArrayList<PageId>> mTIdPageMap = new ConcurrentHashMap<TransactionId, ArrayList<PageId>>();
    ConcurrentHashMap<PageId, LockData> mPageLockMap = new ConcurrentHashMap<PageId, LockData>();

    private synchronized void block(long pStart, long pTimeout) throws TransactionAbortedException 
    {
        if (System.currentTimeMillis() - pStart > pTimeout) 
        {
            throw new TransactionAbortedException();
        }

        try 
        {
            wait(pTimeout);
            if (System.currentTimeMillis() - pStart > pTimeout) 
            {
                throw new TransactionAbortedException();
            }
        } catch (InterruptedException e) 
        {
            e.printStackTrace();
        }
    }

    public synchronized void acquireLock(TransactionId pTId, PageId pPId, LockType pType) throws TransactionAbortedException
    {
        long lStart = System.currentTimeMillis();
        Random lRandom = new Random();
        long lTimeout = lRandom.nextInt(2000);
        while(true) 
        {
        	//Nobody has a lock on the page
        	if (mPageLockMap.containsKey(pPId) == false)
        	{
	        	updateTIdPageMap(pTId, pPId);
	            ArrayList<TransactionId> lListTId = new ArrayList<TransactionId>();
	            lListTId.add(pTId);
	            mPageLockMap.put(pPId, new LockData(pType, lListTId));
	            return;
        	}
    		
        	//Somebody has an Exclusive lock on the page
        	if (mPageLockMap.get(pPId).mLockType == LockType.Xlock)
        	{
        		if (mPageLockMap.get(pPId).mTransactions.get(0) == pTId) 
            	{
            		return;
            	}
        		
            	block(lStart, lTimeout);
            	continue;
        	}
        	
        	//Somebody has a Shared lock on the page
        	
        	if (pType == LockType.Slock) 
            {
        		//Current Transaction is requesting Shared Lock too
                if (mPageLockMap.get(pPId).mTransactions.contains(pTId) == false) 
                {
                    mPageLockMap.get(pPId).mTransactions.add(pTId);
                }
                updateTIdPageMap(pTId, pPId);
                return;
            } 
            else 
            {
            	//Current Transaction is requesting Exclusive Lock
                if(( mPageLockMap.get(pPId).mTransactions.size() == 1 ) 
                		&& ( mPageLockMap.get(pPId).mTransactions.get(0) == pTId ) 
                		&& ( mTIdPageMap.containsKey(pTId) )
                		&& ( mTIdPageMap.get(pTId).contains(pPId) )) 
                {
                    mPageLockMap.get(pPId).mLockType = LockType.Xlock;
                    return;
                } 
                block(lStart, lTimeout);
            }
        }
    }

    private synchronized void updateTIdPageMap(TransactionId pTId, PageId pPId)
    {
        if(mTIdPageMap.containsKey(pTId))
        {
            if(mTIdPageMap.get(pTId).contains(pPId) == false)
            	mTIdPageMap.get(pTId).add(pPId);
            return;
        }
        
        ArrayList<PageId> lPageIdList = new ArrayList<PageId>();
        lPageIdList.add(pPId);
        mTIdPageMap.put(pTId, lPageIdList);
    }

    public synchronized void releaseLock(TransactionId pTId, PageId pPId) 
    {
        if (mTIdPageMap.containsKey(pTId)) 
        {
            mTIdPageMap.get(pTId).remove(pPId);
            if (mTIdPageMap.get(pTId).size() == 0) 
            {
                mTIdPageMap.remove(pTId);
            }
        }
        if (mPageLockMap.containsKey(pPId)) 
        {
            mPageLockMap.get(pPId).mTransactions.remove(pTId);
            if (mPageLockMap.get(pPId).mTransactions.size() == 0) 
            	 mPageLockMap.remove(pPId);
            else 
            	notifyAll();
        }
    }

    public synchronized void releaseLock(TransactionId pTId)
    {
        if(mTIdPageMap.containsKey(pTId))
        {
            PageId[] pages = new PageId[mTIdPageMap.get(pTId).size()];
            PageId[] pagesToRelease = mTIdPageMap.get(pTId).toArray(pages);
            for(PageId pid: pagesToRelease)
            {
                releaseLock(pTId, pid);
            }
        }
    }

    public synchronized boolean holdsLock(TransactionId pTId, PageId pid) 
    {
        if (mTIdPageMap.containsKey(pTId)) 
        {
            if (mTIdPageMap.get(pTId).contains(pid)) {
                return true;
            }
        }
        return false;
    }   
}