import requests

# VM dns names
VMs = ["fa23-cs425-1801.cs.illinois.edu", "fa23-cs425-1802.cs.illinois.edu",
		"fa23-cs425-1803.cs.illinois.edu", "fa23-cs425-1804.cs.illinois.edu",
		"fa23-cs425-1805.cs.illinois.edu", "fa23-cs425-1806.cs.illinois.edu",
		"fa23-cs425-1807.cs.illinois.edu", "fa23-cs425-1808.cs.illinois.edu",
		"fa23-cs425-1809.cs.illinois.edu", "fa23-cs425-1810.cs.illinois.edu"]
Port = 55556

drop_rate = input(">>> Enter new drop rate. Range should be within 0.0 ~ 1.0:\n")

if float(drop_rate) < 0.0 or float(drop_rate) > 1.0:
    print("Not a valid drop rate")


for server in VMs:
    try:
        x = requests.get('http://' + server + f':8080/updateMessageDropRate?value={drop_rate}')
        print(f"{server}: " + x.text)
    except:
        print(f"Unable to connect to {server}")
