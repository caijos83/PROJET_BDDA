package miniSGBDR;

import java.io.Serializable;

public class PageId implements Serializable {
    private static final long serialVersionUID = 1L;
    private int fileIdx;
    private int pageIdx;

    public PageId(int fileIdx, int pageIdx) {
        this.fileIdx = fileIdx;
        this.pageIdx = pageIdx;
    }

    public int getFileIdx() {
        return fileIdx;
    }

    public int getPageIdx() {
        return pageIdx;
    }

    @Override
    public String toString() {
        return "PageId{fileIdx=" + fileIdx + ", pageIdx=" + pageIdx + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof PageId)) return false;
        PageId other = (PageId) obj;
        return this.fileIdx == other.fileIdx && this.pageIdx == other.pageIdx;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(fileIdx) ^ Integer.hashCode(pageIdx);
    }
}
