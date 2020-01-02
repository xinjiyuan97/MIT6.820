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
    private File f;
    private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
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
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pgNo = pid.getPageNumber();
        RandomAccessFile in;
        try {
            in = new RandomAccessFile(f, "r");
            if ((pgNo + 1) * BufferPool.getPageSize() > in.length()) {
                in.close();
                throw new IllegalStateException("Out of file");
            }
            byte[] pagebytes = new byte[BufferPool.getPageSize()];
            in.seek(pgNo * BufferPool.getPageSize());
            if (in.read(pagebytes, 0, BufferPool.getPageSize()) != BufferPool.getPageSize()) 
                throw new IllegalStateException();
            HeapPageId pageId = new HeapPageId(pid.getTableId(), pgNo);
            in.close();
            return new HeapPage(pageId, pagebytes);
        }
        catch(FileNotFoundException e) {}
        catch(IOException e) {}
        throw new IllegalStateException();
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
        return (int) Math.floor(f.length() * 1.0 / BufferPool.getPageSize());
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
    private static class HeapFileIterator implements DbFileIterator {
        private HeapFile hf;
        private TransactionId tid;
        private Iterator<Tuple> it;
        private int index;
        public HeapFileIterator(HeapFile hf, TransactionId tid) {
            this.hf = hf;
            this.tid = tid;
        }

        public Iterator<Tuple> getIterator(int pgNo) throws DbException, TransactionAbortedException {
            if (pgNo >= 0 && pgNo < hf.numPages()) {
                HeapPageId pid = new HeapPageId(hf.getId(), pgNo);
                return ((HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY)).iterator(); 
            }
            else
                throw new DbException("");
        }


        @Override
        public void open() 
            throws DbException, TransactionAbortedException {
            index = 0;
            it = getIterator(index);
        }

        @Override
        public void close() {
            it = null;
        }

        @Override
        public boolean hasNext() 
            throws DbException, TransactionAbortedException {
            if (it == null)
                return false;
            if (!it.hasNext()) {
                if (index < hf.numPages() - 1) {
                    index ++;
                    it = getIterator(index);
                    return it.hasNext();
                }
                else 
                    return false;
            }
            
            return true;
        }

        @Override
        public Tuple next() 
            throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null || !it.hasNext()) {
                throw new NoSuchElementException();
            }
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

    }

    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

