import os
import threading
import format_json
from database import Database
from settings import *


log_filename = os.path.abspath("completed.log")


def file_line_count(file_path, memory_mapped=False):
	print(
		"Reading lines (mapping file to RAM)..." \
			if memory_mapped else                \
		"Reading lines (disk only)..."           \
	)

	if memory_mapped:
		# ALL YOUR RAM IS BELONG TO **BUFFER** >:P
		with open(file_path, "r+") as filename:
			buffer = mmap.mmap(filename.fileno(), 0)
			lines = 0
			while buffer.readline():
				lines += 1
	else:
		# I like having RAM :)
		lines = sum(1 for line in open(file_path))

	return lines

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


class Create_Thread(threading.Thread):
	def __init__(self, thread_num, filename, file_length):
		threading.Thread.__init__(self)
		self.thread_num = thread_num
		self.filename = filename
		self.file_length = file_length

	def run(self):
		# print(self.file_length)
		Database(self.file_length, self.filename).run(self.thread_num)
		# print("Exiting " + self.name)


def main():
	completed = read_file(log_filename)
	filenames = format_json.run()["files"]
	for filename in filenames:
		if filename in completed:
			continue
		file_length = file_line_count(filename)
		threads = []
		for thread_num in range(1, maximum_thread_limit+1):
			threads.append(Create_Thread(thread_num, filename, file_length))
			threads[-1].start()

		for thread in threads:
			thread.join()

		append_file(log_filename, filename)


if __name__ == "__main__":
	main()
