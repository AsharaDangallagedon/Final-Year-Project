import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from datetime import datetime

dates = []
volumeChange = []
with open("part-r-00000", "r") as file:
    for line in file:
        if line.startswith("Volume Rate of Change for: "):
            parts = line.strip().split()
            date = parts[5]
            Volumerateofchange = float(parts[6])
            date = datetime.strptime(date, "%Y-%m-%d")
            dates.append(date)
            volumeChange.append(Volumerateofchange)

plt.figure(figsize=(12, 7))
plt.plot(dates, volumeChange, color="#2a2a2a", marker="o", linestyle="-")
plt.title("Volume Rate of Change over time for this company stock")
plt.xlabel("Date")
plt.ylabel("Daily Volume Rate of Change")
plt.yticks(range(0, 81)) 
plt.grid(True)
plt.savefig("volumerateofchange.png")
