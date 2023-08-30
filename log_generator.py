import random
import datetime
import time
import string


# List of possible service names
services = ['web', 'database', 'authentication', 'email', 'payment']

def generate_log_entry():
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    service = random.choice(services)
    log_level = random.choice(['INFO', 'WARNING', 'ERROR'])
    
    N = 10
    # using random.choices()
    # generating random strings
    res = ''.join(random.choices(string.ascii_letters, k=N))
    return f"{timestamp} - [{service}] [{log_level}] - {res}"

# Number of log entries to generate
num_entries = 10

with open("fake_log.log", "w") as f:
    for _ in range(num_entries):
        log_entry = generate_log_entry()
        f.write(log_entry + "\n")
        time.sleep(0.001)
