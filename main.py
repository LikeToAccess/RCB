import os
import thread
import format_json
from database import Database
from settings import *


log_filename = os.path.abspath("completed.log")


def read_file(filename, filter=True):
	try:
		with open(filename, "r") as file:
			lines = file.read().split("\n")
	except FileExistsError:
		write_file(filename, "")
		return read_file(filename)

	data = []
	for line in lines:
		if line.strip().startswith("#") and filter:
			continue
		data.append(line)

	return data

def write_file(filename, data):
	with open(filename, data) as file:
		file.write(data)

def append_file(filename, data):
	with open(filename, "a") as file:
		file.write(f"{data}\n")

def main():
	completed = read_file(log_filename)
	filenames = format_json.run()["files"]
	for filename in filenames:
		if filename in completed:
			continue
		for thread_num in range(1, maximum_thread_limit+1):
			thread.start_new_thread(
				Database(filename).run,
				(thread_num,)
			)

		append_file(log_filename, filename)


if __name__ == "__main__":
	main()
