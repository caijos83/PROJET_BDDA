package miniSGBDR;

import java.util.List;

public class ProjectOperator implements IRecordIterator {
    private IRecordIterator child;
    private List<Integer> colIndices;

    public ProjectOperator(IRecordIterator child, List<Integer> colIndices) {
        this.child = child;
        this.colIndices = colIndices;
    }

    @Override
    public Record GetNextRecord() {
        Record r = child.GetNextRecord();
        if (r == null) return null;
        Record projected = new Record();
        for (int c : colIndices) {
            projected.addValue(r.getValue(c));
        }
        return projected;
    }

    @Override
    public void Close() {
        child.Close();
    }

    @Override
    public void Reset() {
        child.Reset();
    }
}
