import matplotlib.pyplot as plt

# Configuration names and their corresponding execution times
configs = ['1 Node, 1 Core', '1 Node, 8 Cores', '2 Nodes, 8 Cores']
times = [2031.83, 286.38, 686.53]

# Create a bar chart
plt.figure(figsize=(5, 4))
plt.bar(configs, times, color=['#FF9999', '#66B2FF', '#99FF99'])
plt.title('Execution Time Comparison Across Different Configurations', fontsize=8)
plt.xlabel('Configuration', fontsize=6)
plt.ylabel('Execution Time (seconds)', fontsize=6)

# Save the figure as an image file
plt.savefig('execution_time_comparison.png')
plt.show()
