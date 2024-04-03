echo "Select which problem to test:"
echo "Problem 1"
echo "Problem 2"
echo "Problem 3"
echo "Problem 4"

read -p "Enter your choice (1-4): " choice

case $choice in
    1)
        javac WordCount.java -cp $(hadoop classpath)
        jar cvf wordcount.jar *.class
        hadoop fs -rm -r WordCountOutput
        rm -r WordCountOutput
        hadoop jar wordcount.jar WordCount poems WordCountOutput
        hadoop fs -get WordCountOutput
        cd WordCountOutput
        cat part-r-00000
        ;;
    2)
        javac RegexSearch.java LineNumberInputFormat.java -cp $(hadoop classpath)
        jar cvf regexsearch.jar linenumberinputformat.jar *.class
        hadoop fs -rm -r RegexSearchOutput
        rm -r RegexSearchOutput
        hadoop jar regexsearch.jar RegexSearch philosophy RegexSearchOutput "h\\w+"
        hadoop fs -get RegexSearchOutput
        cd RegexSearchOutput
        cat part-r-00000
        ;;
    3)
        javac NuclearDecay.java -cp $(hadoop classpath)
        jar cvf nucleardecay.jar *.class
        hadoop fs -rm -r NuclearDecayOutput
        rm -r NuclearDecayOutput
        hadoop jar nucleardecay.jar NuclearDecay NuclearDecay.csv NuclearDecayOutput
        hadoop fs -get NuclearDecayOutput
        cd NuclearDecayOutput
        cat part-r-00000
        ;;
    4)
        javac NASDAQ.java -cp $(hadoop classpath)
        jar cvf nasdaq.jar *.class
        ;;
    *)
        echo "Invalid choice. Please choose a number from 1 to 4."
        ;;
esac