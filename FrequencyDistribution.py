import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
#Dictionary to store frequency distribution data
data = {}
#Reading the output file containing frequency distribution data
with open("NuclearDecayDistributionOutput/part-r-00000", "r") as file:
    #iterates through each line of the file and removes whitespace
    for line in file:
        line = line.strip()
        if "\t" in line:
            #splits the line to extract the key and the value
            key, value = map(int, line.split("\t"))
            #key and value (the mass excess uncertainty and its cumulative frequency) is stored in the data dictionary
            data[key] = value
#dictionary used to store the cumulative frequency distribution
frequency_distribution = {}
#calculates the cumulative frequency for the mass excess uncertainty intervals
#I borrowed the below code from these sources:
#https://stackoverflow.com/questions/41551658/how-to-create-a-frequency-distribution-table-on-given-data-with-python-in-jupyte
#https://stackoverflow.com/questions/40825208/python-histogram-how-to-find-the-midpoint-of-bin-with-the-maximum-frequency
#https://www.geeksforgeeks.org/how-to-create-frequency-tables-in-python/
for i in range(3, 26, 2):
    frequency_distribution[i] = sum(data.get(j, 0) for j in range(i, i+2))
#calculates the midpoints of each interval
x_bar = [(range(3, 26, 2)[i] + range(3, 26, 2)[i+1]) / 2 for i in range(len(range(3, 26, 2)) - 1)]
#calculating y-values for the plot
bar_values = [frequency_distribution[x] for x in range(3, 26, 2)[:-1]]
#creating the plot
plt.figure(figsize=(8, 6)) 
#plotting the frequency distribution 
plt.bar(x_bar, bar_values, width=1.2, color= "#2a2a2a")
plt.xticks(range(3, 26, 2))  
#Add title and labels to the plot
plt.xlabel("Mass Excess Uncertainty (in atomic mass units u)")
plt.ylabel("Cumulative Frequency")
plt.title("Frequency Distribution for the Mass Excess Uncertainty of Isotopes of Elements")
#save the plot
plt.savefig("frequency_distribution.png")
