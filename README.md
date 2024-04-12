# Final Year Project

When running WordCount use the following commands:
ssh bigdata
cd /home/cim/ug/zkac422/PROJECT/
javac WordCount.java -cp $(hadoop classpath)
jar cvf wordcount.jar *.class
hadoop jar wordcount.jar WordCount poems WordCountOutput1 

//remember that WordCountOutput1 is the name of the folder and that if your folder is already stored in the hadoop file system, then you will get an error, in order to overcome this issue, you may want to call your folder a different name or you may want to delete the folder from the hadoop system using "hadoop fs -rm -r *nameoffolder*. Moreover, you can check which folders/files are contained in the hadoop file system by using the command hadoop fs -ls 
hadoop fs -get WordCountOutput1
//note that if the folder is already on the local system, you will get an error. In order to avoid this issue, you may want to eliminate the already existing folder using rm -r *nameoffolder* or by just directly deleting it off from the file system
cd WordCountOutput
cat part-r-00000
//use cd .. to go back to the "PROJECT" folder 


When running RegexSearch use the following commands:
javac RegexSearch.java LineNumberInputFormat.java -cp $(hadoop classpath)
jar cvf regexsearch.jar linenumberinputformat.jar *.class
hadoop fs -rm -r RegexSearchOutput
rm -r RegexSearchOutput
hadoop jar regexsearch.jar RegexSearch philosophy RegexSearchOutput "h\\w+"
hadoop fs -get RegexSearchOutput
cd RegexSearchOutput
cat part-r-00000

When running a python file, for example, trading.py, use the following commands:
rm -r tradingrange.png
python tradingrange.py

It is important to note that if you have any doubts regarding how to run a python file, just check the Test.sh script and run any of the commands present in there. 
Moreover, just using the Test script would make it easier to check the results of each file
