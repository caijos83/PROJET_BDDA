package projet_SGBD;

public class PageId {

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

    // Methode d'affichage
    @Override
    public String toString() {
        return "PageId{" +"fileIdx=" + fileIdx +", pageIdx=" + pageIdx +'}';
    }
}