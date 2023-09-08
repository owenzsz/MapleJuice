import os
import random
import datetime
import re
import string
import socket
import rstr


'''
Patterns that will form the fake log file. 
These patterns will be used to test the distributed log querier system
'''
infreq_1 = "infreq_1jweqoifniosdanfoinasodifnoiqwefq"                               # infrequent
infreq_2 = "infreq_2ajsdoifjqewoifjqwesadfads"                                      # infrequent
freq_1 = "payment"                                                                  # frequent
freq_2 = "WARNING"                                                                  # frequent
regex_1 = r"[0-9]+-[0-9]+-[0-9]+ "                                                  # frequent
regex_2 = r"[Aa][pP][pP][lL][eE] [mM][aA][cC][iI][nN][tT][oO][sS][hH] (abc|123)"    # frequent
only_once_1 = "fa23-cs425-1802.cs.illinois.edu"                                     # unique to 1802
only_once_2 = "fa23-cs425-1803.cs.illinois.edu"                                     # unique to 1803
all_machines = "CS_425 MP1"                                                         # presented on all VMs
patterns = [infreq_1, infreq_2, freq_1, freq_2, regex_1, regex_2, only_once_1, only_once_2, all_machines]

# List of possible service names
services = ['web', 'database', 'authentication', 'email', 'payment']
# VM dns names
VMs = ["fa23-cs425-1801.cs.illinois.edu", "fa23-cs425-1802.cs.illinois.edu",
		"fa23-cs425-1803.cs.illinois.edu", "fa23-cs425-1804.cs.illinois.edu",
		"fa23-cs425-1805.cs.illinois.edu", "fa23-cs425-1806.cs.illinois.edu",
		"fa23-cs425-1807.cs.illinois.edu", "fa23-cs425-1808.cs.illinois.edu",
		"fa23-cs425-1809.cs.illinois.edu", "fa23-cs425-1810.cs.illinois.edu"]
Port = 55555

# Statistics of generated lines
vm_reference_stats = [dict() for _ in range(10)]
global_reference_stats = dict()


# Utility class modified from https://enzircle.hashnode.dev/handling-message-boundaries-in-socket-programming
class Buffer(object):
    def __init__(self, sock):
        self.sock = sock
        self.buffer = b""
        self.prev_line = ""

    def get_line(self):
        while b"\n" not in self.buffer:
            data = self.sock.recv(4096)
            if not data: # socket is closed
                return None
            self.buffer += data
        line, sep, self.buffer = self.buffer.partition(b"\n")
        self.prev_line = line.decode()
        return self.prev_line


# Randomly generate a log entry
def generate_log_entry():
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    service = random.choice(services)
    log_level = random.choice(['INFO', 'WARNING', 'ERROR'])
    
    # generating random strings
    coin = random.randint(0, 10)
    if (coin > 2):
        N = 50
        res = ''.join(random.choices(string.ascii_letters, k=N))
    else:
        res = rstr.xeger(regex_2)
    return f"{timestamp} - [{service}] [{log_level}] - {res}"


# Count occurrances of matched patterns in log that's stored on machine numbered machine_index
def count_matched_occurrances(machine_index, r_str):
    dictionary = vm_reference_stats[machine_index-1]
    for ptrn in patterns:
        if re.search(ptrn, r_str):
            # Add occurrance to machine-specific counter
            if ptrn in dictionary:
                dictionary[ptrn] += 1
            else:
                dictionary[ptrn] = 1

            # Add occurrance to global counter
            if ptrn in global_reference_stats:
                global_reference_stats[ptrn] += 1
            else:
                global_reference_stats[ptrn] = 1


# Number of log entries to generate
num_entries = 2000

for i in range(1, 11):
    with open(f"./fake_log{i}.log", "w+") as f:
        f.write("Test_Log" + "\n")

        title = "CS_425 MP1"
        f.write(title + "\n")
        count_matched_occurrances(i, title)
        
        hostName = f"fa23-cs425-18{str(i).zfill(2)}.cs.illinois.edu"
        f.write("Host name is: " + hostName + "\n")
        count_matched_occurrances(i, hostName)
        for _ in range(num_entries):
            string_written = ""
            # About 5% chance to execute this branch
            if random.randint(0, 100) < 5:
                if random.randint(0, 1) == 0:
                    string_written = infreq_1
                    
                else:
                    string_written = infreq_2
            # About 95% chance to execute this branch
            else:
                if random.randint(0, 1) == 0:
                    r_str = generate_log_entry()
                    string_written = r_str
                else:
                    # use strings that fit some special regex
                    r_str = rstr.xeger(regex_2)
                    string_written = r_str
            f.write(string_written+"\n")
            count_matched_occurrances(i, string_written)
    # send to machine index i-1
    os.system(f"scp ./fake_log{i}.log cs425-{i}:~/cs425-mp1/fake_log.log")


# For each machine, qeury each pattern and compare with locally calculated counts
for i in range(1, 11):
    num_all_mached_patterns = 0
    current_VM_reference_stat = vm_reference_stats[i-1]
    for pattern in current_VM_reference_stat:
        match_count = 0
        reference_count = current_VM_reference_stat[pattern]

        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Connect the socket to the port where the server is listening
        server_address = (VMs[i-1], Port)
        try:
            sock.connect(server_address)
        
            # Send data
            message = f'grep -E "{pattern}" ./fake_log.log\n'
            sock.sendall(message.encode())

            buffered_sock = Buffer(sock)
            while True: 
                line = buffered_sock.get_line()
                if line is None:
                    break                
                if re.search(pattern, line):
                    match_count += 1
            if (match_count != reference_count):
                print(f"Incorrect number of matched lines for pattern: {pattern}, reference count is: {reference_count}, and match count is: {match_count}")
            else:
                print(f"Correct number of matched lines for pattern: {pattern}")
                num_all_mached_patterns += 1
        except IOError:
            pass
        finally:
            sock.close()
    # print whether a VM gives all the correct counts
    if num_all_mached_patterns == len(current_VM_reference_stat.keys()):
        print(f"VM{i} gives all correct counts\n")

        


