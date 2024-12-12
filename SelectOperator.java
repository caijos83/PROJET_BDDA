package miniSGBDR;

import java.util.List;

public class SelectOperator implements IRecordIterator {
    private IRecordIterator child;
    private List<Condition> conditions;
    private Relation relation;

    public SelectOperator(Relation relation, IRecordIterator child, List<Condition> conditions) {
        this.child = child;
        this.conditions = conditions;
        this.relation = relation;
    }

    @Override
    public Record GetNextRecord() {
        Record r;
        while ((r = child.GetNextRecord()) != null) {
            boolean ok = true;
            for (Condition c : conditions) {
                if (!c.evaluate(r, relation)) {
                    ok = false;
                    break;
                }
            }
            if (ok) return r;
        }
        return null;
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
