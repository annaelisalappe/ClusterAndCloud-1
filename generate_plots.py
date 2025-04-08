import matplotlib.pyplot as plt

# Configuration names and their corresponding execution times
configs = ['1 Node, 1 Core', '1 Node, 8 Cores', '2 Nodes, 8 Cores']
times = [0.0466, 0.0346, 0.0353]

# Create a bar chart
plt.figure(figsize=(5, 4))
plt.bar(configs, times, color=['#FF9999', '#66B2FF', '#99FF99'])
plt.title('Average Execution Time Comparison on 16m Dataset', fontsize=8)
plt.xlabel('Configuration', fontsize=6)
plt.ylabel('Average Execution Time (seconds)', fontsize=6)

# Save the figure as an image file
plt.savefig('benchmark_time_comparison.png')
plt.show()
