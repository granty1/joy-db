package simpledb;

import java.io.*;
import java.util.*;

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

    public class HeapFileIterator implements DbFileIterator {
        private int curPageNo;
        private HeapPage curPage;
        private TransactionId transactionId;
        private int tableId;
        private Iterator<Tuple> tupleIterator;

        public HeapFileIterator(TransactionId transactionId) {
            this.transactionId = transactionId;
            this.tableId = getId();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.curPageNo = 0;
            resetPage();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.tupleIterator == null) return false;
            if (this.tupleIterator.hasNext()) return true;

            while (this.curPageNo + 1 < numPages()) {
                // move to next page
                this.curPageNo++;
                resetPage();
                if (this.tupleIterator.hasNext()) return true;
            }

            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (this.tupleIterator == null || !this.tupleIterator.hasNext()) {
                throw new NoSuchElementException();
            }

            return this.tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            this.curPageNo = 0;
            this.curPage = null;
            this.tupleIterator = null;
        }

        private void resetPage() throws DbException, TransactionAbortedException  {
            HeapPageId pageId = new HeapPageId(this.tableId, this.curPageNo);
            this.curPage = (HeapPage) Database.getBufferPool().getPage(this.transactionId, pageId, Permissions.READ_ONLY);
            this.tupleIterator = this.curPage.iterator();
        }
    }

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsolutePath().hashCode() % 100000;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        Page page = null;
        HeapPageId heapPageId = new HeapPageId(pid.getTableId(), pid.getPageNumber());
        byte[] b = new byte[BufferPool.getPageSize()];
        try {
            if (BufferPool.getPageSize() * pid.getPageNumber() >= this.file.length()) {
                // new page
                for (int i = 0; i < b.length; i++) {
                    b[i] = 0;
                }
                page = new HeapPage(new HeapPageId(getId(), numPages()), b);
                writePage(page);
            } else {
                FileInputStream fis = new FileInputStream(this.file);
                // skip the size * number bytes
                fis.skip(pid.getPageNumber() * BufferPool.getPageSize());
                fis.read(b, 0, BufferPool.getPageSize());
                page = new HeapPage(heapPageId, b);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

