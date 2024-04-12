echo "Select which type of files to test:"
echo "1) Java Files"
echo "2) Python Files"

read -p "Enter your choice (1 or 2): " fileChoice

case $fileChoice in
    1)
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
                echo "Which task should I perform for Nuclear Decay?"
                echo "1) Frequency Distribution"
                echo "2) Order Statistics Calculations"
                read -p "Enter your choice (1-2): " taskChoice

                if [ "$taskChoice" = "1" ]; then
                    javac NuclearDecayDistribution.java -cp $(hadoop classpath)
                    jar cvf nucleardecaydistribution.jar *.class
                    hadoop fs -rm -r NuclearDecayDistributionOutput
                    rm -r NuclearDecayDistributionOutput
                    hadoop jar nucleardecaydistribution.jar NuclearDecayDistribution NuclearDecay.csv NuclearDecayDistributionOutput
                    hadoop fs -get NuclearDecayDistributionOutput
                    cd NuclearDecayDistributionOutput
                    cat part-r-00000
                    
                elif [ "$taskChoice" = "2" ]; then
                    javac NuclearDecay.java -cp $(hadoop classpath)
                    jar cvf nucleardecay.jar *.class
                    hadoop fs -rm -r NuclearDecayOutput
                    rm -r NuclearDecayOutput
                    hadoop jar nucleardecay.jar NuclearDecay NuclearDecay.csv NuclearDecayOutput
                    hadoop fs -get NuclearDecayOutput
                    cd NuclearDecayOutput
                    cat part-r-00000
                else
                    echo "Invalid choice. Please choose a number of either 1 or 2."
                fi
                ;;
            4)
                echo "Which Metric should be calculated for Problem 4?"
                echo "1) Trading Range"
                echo "2) Price Change"
                echo "3) Volume Rate of Change"
                echo "4) RSI"
                read -p "Enter your choice (1-4): " taskChoice

                if [ "$taskChoice" = "1" ]; then
                    javac TradingRange.java -cp $(hadoop classpath)
                    jar cvf tradingrange.jar *.class
                    hadoop fs -rm -r RangeOutput
                    rm -r RangeOutput
                    hadoop jar tradingrange.jar TradingRange Stock_Data/stocks/A.csv RangeOutput
                    hadoop fs -get RangeOutput
                    cd RangeOutput
                    cat part-r-00000
                    
                elif [ "$taskChoice" = "2" ]; then
                    javac PriceChange.java -cp $(hadoop classpath)
                    jar cvf pricechange.jar *.class
                    hadoop fs -rm -r PriceChangeOutput
                    rm -r PriceChangeOutput
                    hadoop jar pricechange.jar PriceChange Stock_Data/stocks/A.csv PriceChangeOutput
                    hadoop fs -get PriceChangeOutput
                    cd PriceChangeOutput
                    cat part-r-00000

                elif [ "$taskChoice" = "3" ]; then
                    javac VolumeChange.java -cp $(hadoop classpath)
                    jar cvf volumechange.jar *.class
                    hadoop fs -rm -r VolumeRateofChangeOutput
                    rm -r VolumeRateofChangeOutput
                    hadoop jar volumechange.jar VolumeChange Stock_Data/stocks/A.csv VolumeRateofChangeOutput
                    hadoop fs -get VolumeRateofChangeOutput
                    cd VolumeRateofChangeOutput
                    cat part-r-00000
                 
                elif [ "$taskChoice" = "4" ]; then
                    javac RSI.java -cp $(hadoop classpath)
                    jar cvf rsi.jar *.class
                    hadoop fs -rm -r RSIoutput
                    rm -r RSIoutput
                    hadoop jar rsi.jar RSI Stock_Data/stocks/A.csv RSIoutput
                    hadoop fs -get RSIoutput
                    cd RSIoutput
                    cat part-r-00000
                else
                    echo "Invalid choice. Please choose a number from 1 to 4."
                fi
                ;;
            *)
                echo "Invalid choice. Please choose a number from 1 to 4."
                ;;
        esac
        ;;

    2) 
        echo "Select which python file to test:"
        echo "1) FrequencyDistribution.py"
        echo "2) tradingrange.py"
        echo "3) pricechange.py"
        echo "4) volumerateofchange.py"
        echo "5) rsi.py"

        read -p "Enter your choice (1-5): " choice

        case $choice in
            1) 
                rm -r frequency_distribution.png
                python FrequencyDistribution.py
                ;;
            2)
                rm -r tradingrange.png
                python tradingrange.py
                ;;
            3) 
                rm -r change.png
                python pricechange.py
                ;;
            4)
                rm -r volumerateofchange.png
                python volumerateofchange.py
                ;;
            5)
                rm -r RSI.png
                python rsi.py
                ;;
            *)
                echo "Invalid choice. Please choose a number from 1 to 5."
                ;;
        esac
        ;;
    *)
        echo "Invalid choice. Please choose a number from 1 to 2."
        ;;
esac