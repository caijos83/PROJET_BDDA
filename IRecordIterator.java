public interface IRecordIterator {
    //Méthode qui retourne le record courant ou null s'il n'y a plus de record.
    Record getNextRecord();

    //Métode qui ferme l'itérateur et libère toutes les ressources associées.
    void close();

    //Méthode qui réinitialise le curseur au début de l'ensemble des records.
    void reset();
}
