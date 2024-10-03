package projet_SGBD;
import java.io.Serializable;
public class PageId implements Serializable{
	private static final long serialversionUID=1l;
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