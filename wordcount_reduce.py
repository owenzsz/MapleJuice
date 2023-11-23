import sys

def process_line(line):
    kv = line.strip().split(":")
    key, value_set = kv[0], kv[1]
    agg_result = sum([int(x) for x in value_set.split(",")])
    print(f"{key}:{agg_result}")


if __name__ == "__main__":
    for line in sys.stdin:
        process_line(line)