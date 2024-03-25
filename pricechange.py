import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from datetime import datetime

dates = []
priceChange = []
with open("PriceChangeOutput/part-r-00000", "r") as file:
    for line in file:
        if line.startswith("Price Change for: "):
            parts = line.strip().split()
            date = parts[3]
            changedata = float(parts[4])
            date = datetime.strptime(date, "%Y-%m-%d")
            dates.append(date)
            priceChange.append(changedata)

plt.figure(figsize=(12, 7))
plt.plot(dates, priceChange, color="#2a2a2a", marker="o", linestyle="-")
plt.title("Price Change over time for this company stock")
plt.xlabel("Date")
plt.ylabel("Daily Price Change (change in $)")
plt.yticks(range(-36,19,2)) 
plt.grid(True, which="both")
plt.savefig("change.png")
