package miniSGBDR;

public class RecordPrinter {
    private IRecordIterator iter;
    private Relation relation;

    public RecordPrinter(IRecordIterator iter) {
        this.iter = iter;
    }

    public void printAll() {
        int count = 0;
        Record r;
        while ((r = iter.GetNextRecord()) != null) {

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < r.size(); i++) {
                if (i > 0) sb.append(" ; ");
                sb.append(r.getValue(i));
            }
            sb.append(".");
            System.out.println(sb.toString());
            count++;
        }
        System.out.println("Total records=" + count);
    }
}
