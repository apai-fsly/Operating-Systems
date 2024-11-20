import matplotlib.pyplot as plt
from datetime import datetime

def time_to_seconds(time_str):
    time_obj = datetime.strptime(time_str, "%M:%S.%f")
    return time_obj.minute * 60 + time_obj.second + time_obj.microsecond / 1e6

file_name = 'times.txt'

# Read the times from a file
with open(file_name, 'r') as file:
    times = file.readlines()

# Remove any trailing newlines or spaces
times = [time.strip() for time in times]

# Convert the list of times to seconds
times_in_seconds = [time_to_seconds(t) for t in times]

# Subtract the first time point to start at 0
start_time = times_in_seconds[0]
times_in_seconds = [t - start_time for t in times_in_seconds]

# Create the line plot with switched axes
plt.plot(range(len(times_in_seconds)), times_in_seconds, marker='o', color='blue')

plt.xlabel('Number of Requests')
plt.ylabel('Time (seconds)')
plt.title('Time Series Plot')
plt.grid(True)
plt.show()
