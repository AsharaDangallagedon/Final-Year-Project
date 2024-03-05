import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

data = {}
with open("part-r-00000", "r") as file:
    for line in file:
        line = line.strip()
        if "\t" in line:
            key, value = map(int, line.split('\t'))
            data[key] = value

frequency_distribution = {}
for i in range(3, 26, 2):
    frequency_distribution[i] = sum(data.get(j, 0) for j in range(i, i+2))
x_bar = [(range(3, 26, 2)[i] + range(3, 26, 2)[i+1]) / 2 for i in range(len(range(3, 26, 2)) - 1)]
bar_values = [frequency_distribution[x] for x in range(3, 26, 2)[:-1]]

plt.figure(figsize=(7, 5)) 
plt.bar(x_bar, bar_values, width=1.2, color= "#2a2a2a")
plt.xticks(range(3, 26, 2))  
plt.xlabel("Mass Excess Uncertainty")
plt.ylabel("Cumulative Frequency")
plt.title("Frequency Distribution")
plt.savefig("frequency_distribution.png")
