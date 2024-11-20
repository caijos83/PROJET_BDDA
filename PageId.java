package projet_SGBD;

public class PageId {
    
    private int FileIdx;
    private int PageIdx;
    
    public PageId(int FileIdx, int PageIdx) {
        this.FileIdx = FileIdx;
        this.PageIdx = PageIdx;
    }

    public int getFileIdx() {
    	return FileIdx;
    }
    
    public int getPageIdx() {
    	return PageIdx;
    }
    
    public void setFileIdx(int FileIdx) {
    	this.FileIdx =  FileIdx;
    }
    
    public void setPageIdx(int PageIdx) {
    	this.PageIdx = PageIdx;
    }
}
