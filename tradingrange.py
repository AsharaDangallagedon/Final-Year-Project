import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from datetime import datetime

dates = []
priceRange = []
with open("RangeOutput/part-r-00000", "r") as file:
    for line in file:
        if line.startswith("Trading Range for: "):
            parts = line.strip().split()
            date = parts[3]
            rangedata = float(parts[4])
            date = datetime.strptime(date, "%Y-%m-%d")
            dates.append(date)
            priceRange.append(rangedata)

plt.figure(figsize=(12, 7))
plt.plot(dates, priceRange, color="#2a2a2a", marker="o", linestyle="-")
plt.title("Trading Range over time for this company stock")
plt.xlabel("Date")
plt.ylabel("Daily Trading Range (change in $)")
plt.yticks(range(0, 41)) 
plt.grid(True)
plt.savefig("tradingrange.png")
