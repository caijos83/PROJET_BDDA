
public class RecordID {
	private PageId pageId;
	private int slotIdx;
    public RecordID(PageId pageId, int slotIdx) {
        this.pageId = pageId;
        this.slotIdx = slotIdx;
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof RecordID)) return false;
        RecordID that = (RecordID) obj;
        return slotIdx == that.slotIdx && pageId.equals(that.pageId);
    }

    @Override
    public int hashCode() {
        int result = pageId.hashCode();
        result = 31 * result + slotIdx;
        return result;
    }
}
