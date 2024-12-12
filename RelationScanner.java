package miniSGBDR;

import java.util.List;

public class RelationScanner implements IRecordIterator {
    private List<Record> allRecords;
    private int currentIdx;

    public RelationScanner(Relation rel) throws Exception {
        this.allRecords = rel.GetAllRecords();
        this.currentIdx = 0;
    }

    @Override
    public Record GetNextRecord() {
        if (currentIdx < allRecords.size()) {
            return allRecords.get(currentIdx++);
        }
        return null;
    }

    @Override
    public void Close() {
        
    }

    @Override
    public void Reset() {
        currentIdx = 0;
    }
}
