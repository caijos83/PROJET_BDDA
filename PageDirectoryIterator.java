package miniSGBDR;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;

public class PageDirectoryIterator {
    private BufferManager bufferManager;
    private int currentIndex;
    private PageId headerPage;

    public PageDirectoryIterator(BufferManager bufferManager, PageId headerPage) {
        this.bufferManager = bufferManager;
        currentIndex = 0;
        this.headerPage = headerPage;

    }

    public PageId getNextDataPageId() throws IOException {
        ByteBuffer headerBuffer = bufferManager.getPage(headerPage);
        int numPages = headerBuffer.getInt(0);
        if (currentIndex >= numPages) {
            bufferManager.freePage(headerPage, false);
            return null;
        }
        int pageOffset = 4 + currentIndex * 12; // 4 octets pour le nombre de pages + 12 octets par entrée (PageId + espace libre)
        int fileId = headerBuffer.getInt(pageOffset);
        int pageNumber = headerBuffer.getInt(pageOffset + 4);

        currentIndex++;
        return new PageId(fileId, pageNumber);

    }

    public PageId getHeaderPage() {
        return headerPage;
    }

    public int getCurrentIndex() {
        return currentIndex;
    }

    public void setCurrentIndex(int currentIndex) {
        this.currentIndex = currentIndex;
    }

    public void setHeaderPage(PageId headerPage) {
        this.headerPage = headerPage;
    }

    public void reset(){};
    public void close(){};
}
