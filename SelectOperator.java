//import java.io.IOException;
import java.util.List;

public class SelectOperator implements IRecordIterator {
    private IRecordIterator fils;
    private List<Condition> conditions;
    private Record recordCourant;
    private boolean hasMoreRecords;   

    public SelectOperator(IRecordIterator fils, List<Condition> conditions) {
        this.fils = fils;
        this.conditions = conditions;
        this.hasMoreRecords = true;
    }
    public IRecordIterator getFils(){
        return fils;
    }
    @Override
    public Record getNextRecord() {
        while (hasMoreRecords) {
            // Récupérer le prochain record de l'itérateur enfant
            recordCourant = fils.getNextRecord();

            // Si l'itérateur a retourné null, cela signifie qu'il n'y a plus de records
            if (recordCourant == null) {
                hasMoreRecords = false;
                return null;
            }

            boolean matches = true;
            for (Condition condition : conditions) {
                if (!condition.evaluate(recordCourant, null, null)) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                return recordCourant;
            }
        }
        // Si aucun record ne satisfait la condition ou s'il n'y a plus de records
        return null;
    }

    @Override
    public void close() {
        fils.close(); // Délègue la fermeture à l'opérateur fils
    }

    @Override
    public void reset() {
        fils.reset(); // Réinitialise l'opérateur fils
        hasMoreRecords = true;
    }
}
