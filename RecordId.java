package miniSGBDR;


public class RecordId {
	private PageId pageId;
	private int slotIdx;
    public RecordId(PageId pageId, int slotIdx) {
        this.pageId = pageId;
        this.slotIdx = slotIdx;
    }
    public RecordId() {
        this.pageId = null;
        this.slotIdx = 0;

    }
    public PageId getPageId() {
        return pageId;
    }

    public int getSlotIdx() {
        return slotIdx;
    }
    public void setPageId(PageId pageId) {
        this.pageId=pageId;
    }

    public void getSlotIdx(int slotIdx) {
        this.slotIdx= slotIdx;
    }

    @Override
    public String toString() {
        return "RecordID{" +"pageId=" + pageId + ", slotIdx=" + slotIdx + '}';
    }

}
