

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.lang.Exception;

public class BufferManager {
    private final DBConfig dbConfig;
    private final DiskManager diskManager;
    private final Map<PageId, ByteBuffer> bufferPool;
    private int pin_count;

    private final Map<PageId, Boolean>  flag_dirty;

    // Constructor
    public BufferManager(DBConfig dbConfig, DiskManager diskManager) {
        this.dbConfig = dbConfig;
        this.diskManager = diskManager;
        this.bufferPool = new HashMap<>();
        this.flag_dirty = new HashMap<>();
    }

    // Method to get a page
    public ByteBuffer getPage(PageId pageId) throws IOException {
        ByteBuffer buffer = bufferPool.get(pageId);


        // If page is not in the buffer, read from disk and apply replacement policy if needed
        if (buffer == null) {
            if (bufferPool.size()<dbConfig.getBm_buffercount()){
                buffer = ByteBuffer.allocate(dbConfig.getPagesize());
                diskManager.readPage(pageId, buffer);
                bufferPool.put(pageId,buffer);

            }else {
                buffer = ByteBuffer.allocate(dbConfig.getPagesize());
                diskManager.readPage(pageId, buffer);
                applyReplacementPolicy(pageId, buffer);
            }



        }
        pin_count++;

        return buffer;
    }

    private void applyReplacementPolicy(PageId pageId, ByteBuffer buffer) {
        if (bufferPool.size() >= dbConfig.getBm_buffercount()) {
            PageId pageToEvict = findPageToEvict();
            bufferPool.remove(pageToEvict);
        }
        bufferPool.put(pageId, buffer);
    }

    private PageId findPageToEvict() {
        if (dbConfig.getBm_policy() == DBConfig.BMpolicy.LRU) {
            return findLeastRecentlyUsedPage();
        } else { // MRU
            return findMostRecentlyUsedPage();
        }
    }

    private PageId findMostRecentlyUsedPage() {
        // MRU logic here (simplified for demonstration)
        return bufferPool.keySet().iterator().next();
    }

    private PageId findLeastRecentlyUsedPage() {
        // LRU logic here (simplified for demonstration)

        PageId leastRecent = null;
        for (PageId pageId : bufferPool.keySet()) {
            leastRecent = pageId;
        }
        return leastRecent;
    }

    // Method to free a page
    public void freePage(PageId pageId, boolean isDirty) {
        pin_count = Math.max(0, pin_count - 1); // Decrement pin_count without going below 0
        flag_dirty.put(pageId,isDirty);

        if (isDirty) {
            ByteBuffer buffer = bufferPool.get(pageId);
            if (buffer != null) {
                bufferPool.put(pageId, buffer); // Update buffer pool with dirty flag
            }
        }
    }

    // Method to set current replacement policy
    public void setCurrentReplacementPolicy(DBConfig.BMpolicy policy) {
        dbConfig.bm_policy = policy;
    }

    // Method to flush all buffers to disk
    public void flushBuffers() throws IOException {
        for (Map.Entry<PageId, ByteBuffer> entry : bufferPool.entrySet()) {
            PageId pageId = entry.getKey();
            ByteBuffer buffer = entry.getValue();

            if (Boolean.TRUE.equals(this.flag_dirty.get(pageId) ) ) {
                diskManager.writePage(pageId, buffer);
            }
        }
        bufferPool.clear(); // Reset buffers
        flag_dirty.clear();
        pin_count = 0;

    }

    public Boolean getFlagDirty(PageId pageId) throws IOException{
        Boolean r = flag_dirty.get(pageId);
        if (r == null) {
            throw new IOException("La pageId n'existe pas");
        }
        return r;
    }
    public Boolean isLoad(PageId pageId){
        if (bufferPool.containsKey(pageId)) {
            return true;
        }else{
            return false;
        }
    }
}
