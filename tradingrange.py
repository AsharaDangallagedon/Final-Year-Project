import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from datetime import datetime
#Lists to store dates and the corresponding price ranges
dates = []
priceRange = []
#Reading the output file containing trading range data
with open("RangeOutput/part-r-00000", "r") as file:
    for line in file:
        #Check if the line contains trading range data
        if line.startswith("Trading Range for: "):
            #Split the line to extract date and trading range
            parts = line.strip().split()
            date = parts[3]
            rangedata = float(parts[4])
            #Parse the date String into a datetime object
            date = datetime.strptime(date, "%Y-%m-%d")
            #Append date and trading range to their respective lists
            dates.append(date)
            priceRange.append(rangedata)
#Creating a plot
plt.figure(figsize=(12, 7))
#Plot the trading range over time
plt.plot(dates, priceRange, color="#2a2a2a", marker="o", linestyle="-")
#Add title and labels to the plot
plt.title("Trading Range over time for this company stock")
plt.xlabel("Date")
plt.ylabel("Daily Trading Range (change in $)")
#Setting the y-axis ticks
plt.yticks(range(0, 41)) 
plt.grid(True)
#Save the plot
plt.savefig("tradingrange.png")
