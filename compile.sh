rm -rf bin/
mkdir -p bin
javac -d bin  *.java
java -cp bin SGBD
