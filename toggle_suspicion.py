import requests

# VM dns names
VMs = ["fa23-cs425-1801.cs.illinois.edu", "fa23-cs425-1802.cs.illinois.edu",
		"fa23-cs425-1803.cs.illinois.edu", "fa23-cs425-1804.cs.illinois.edu",
		"fa23-cs425-1805.cs.illinois.edu", "fa23-cs425-1806.cs.illinois.edu",
		"fa23-cs425-1807.cs.illinois.edu", "fa23-cs425-1808.cs.illinois.edu",
		"fa23-cs425-1809.cs.illinois.edu", "fa23-cs425-1810.cs.illinois.edu"]
Port = 55556


for server in VMs:
    try:
        x = requests.get('http://' + server + ':8080/toggleSuspicion')
        print(f"{server}: " + x.text)
    except:
        print(f"Unable to connect to {server}")


