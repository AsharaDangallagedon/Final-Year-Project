import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from datetime import datetime
#Lists to store dates and the corresponding volume rate of change values
dates = []
volumeChange = []
#Reading the output file containing the volume rate of change data
with open("VolumeRateofChangeOutput/part-r-00000", "r") as file:
    for line in file:
        #Check if the line contains volume rate of change data
        if line.startswith("Volume Rate of Change for: "):
            #Split the line to extract date and the volume rate of change
            parts = line.strip().split()
            date = parts[5]
            Volumerateofchange = float(parts[6])
            #Parse the date String into a datetime object
            date = datetime.strptime(date, "%Y-%m-%d")
            #Append date and VROC to their respective lists
            dates.append(date)
            volumeChange.append(Volumerateofchange)
#Creating a plot
plt.figure(figsize=(12, 7))
#Plot the volume rate of change over time
plt.plot(dates, volumeChange, color="#2a2a2a", marker="o", linestyle="-")
#Add title and labels to the plot
plt.title("Volume Rate of Change for this company stock")
plt.xlabel("Date")
plt.ylabel("Daily Volume Rate of Change (change in %)")
#Setting the y-axis ticks
plt.yticks(range(-90, 1000, 50)) 
plt.grid(True)
#Save the plot
plt.savefig("volumerateofchange.png")
