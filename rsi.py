import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from datetime import datetime

dates = []
priceRange = []
with open("RSIoutput/part-r-00000", "r") as file:
    for line in file:
        if line.startswith("RSI for: "):
            parts = line.strip().split()
            date = parts[2]
            rangedata = float(parts[3])
            date = datetime.strptime(date, "%Y-%m-%d")
            dates.append(date)
            priceRange.append(rangedata)
plt.figure(figsize=(12, 7))
plt.plot(dates, priceRange, color="#2a2a2a", marker="o", linestyle="-")
plt.title("RSI for this company stock")
plt.xlabel("Date")
plt.ylabel("RSI")
plt.yticks(range(0, 100, 5)) 
plt.grid(True)
plt.savefig("RSI.png")
