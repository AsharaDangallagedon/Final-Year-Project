import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from datetime import datetime
#Lists to store dates and the corresponding rsi values
dates = []
rsi = []
#Reading the output file containing rsi data
with open("RSIoutput/part-r-00000", "r") as file:
    for line in file:
        #Check if the line contains rsi data
        if line.startswith("RSI for: "):
            #Split the line to extract date and rsi
            parts = line.strip().split()
            date = parts[2]
            rsidata = float(parts[3])
            #Parse the date String into a datetime object
            date = datetime.strptime(date, "%Y-%m-%d")
            #Append date and rsi to their respective lists
            dates.append(date)
            rsi.append(rsidata)
#Creating a plot
plt.figure(figsize=(12, 7))
#Plot the rsi over time
plt.plot(dates, rsi, color="#2a2a2a", marker="o", linestyle="-")
#Add title and labels to the plot
plt.title("RSI for this company stock")
plt.xlabel("Date")
plt.ylabel("RSI")
#Setting the y-axis ticks
plt.yticks(range(0, 105, 5))
#visualise the oversold and overbought areas
plt.axhline(70, color="red", linestyle="-", linewidth=3)
plt.axhline(30, color="green", linestyle="-", linewidth=3)
plt.grid(True)
#Save the plot
plt.savefig("RSI.png")