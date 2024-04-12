import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from datetime import datetime
#Lists to store dates and the corresponding price change values
dates = []
priceChange = []
#Reading the output file containing price change data
with open("PriceChangeOutput/part-r-00000", "r") as file:
    for line in file:
        #Check if the line contains price change data
        if line.startswith("Price Change for: "):
            #Split the line to extract date and the price change
            parts = line.strip().split()
            date = parts[3]
            changedata = float(parts[4])
            #Parse the date String into a datetime object
            date = datetime.strptime(date, "%Y-%m-%d")
            #Append date and price change to their respective lists
            dates.append(date)
            priceChange.append(changedata)
#Creating a plot
plt.figure(figsize=(12, 7))
#Plot the price change over time
plt.plot(dates, priceChange, color="#2a2a2a", marker="o", linestyle="-")
#Add title and labels to the plot
plt.title("Price Change over time for this company stock")
plt.xlabel("Date")
plt.ylabel("Daily Price Change (change in $)")
#Setting the y-axis ticks
plt.yticks(range(-36,19,2)) 
plt.grid(True, which="both")
#Save the plot
plt.savefig("change.png")
