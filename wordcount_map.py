import sys

def process_line(line):
    words = line.strip().split()
    for word in words:
        print(f"{word}:1")

if __name__ == "__main__":
    for line in sys.stdin:
        process_line(line)