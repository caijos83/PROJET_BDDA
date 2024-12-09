import java.util.ArrayList;
import java.util.List;

public class ProjectOperator implements IRecordIterator {
    private IRecordIterator fils;// Opérateur fils
    private List<Condition> conditions;
    private boolean hasMoreRecords;  

    public ProjectOperator( SelectOperator op, List<Condition> conditions) {
        fils = op.getFils();
        this.conditions = conditions;
        this.hasMoreRecords = true;
    }
    public IRecordIterator getFils(){
        return fils;
    }

    @Override
    public Record getNextRecord() {
        Record record =fils.getNextRecord();
        if (record == null) {
            return null;
        }

        Record projectedRecord = new Record();

        for (Condition condition : conditions) {
            
            projectedRecord.addValue(condition.getColumnIndex());
        }

         // Crée un nouveau tuple ne contenant que les colonnes sélectionnées
         List<Object> projectedValues = new ArrayList<>(conditions.size());
         for (int i = 0; i < conditions.size(); i++) {
             projectedValues.add(conditions.get(i)) ;
         }
 

        // Retourne le tuple projeté
        return new Record(projectedValues);
    }

    @Override
    public void close() {
        fils.close(); // Délègue la fermeture à l'opérateur fils
    }

    @Override
    public void reset() {
        fils.reset(); // Réinitialise l'opérateur fils
    }
}
