import os
import threading
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


class Create_Thread(threading.Thread):
	def __init__(self, thread_num, filename):
		threading.Thread.__init__(self)
		self.thread_num = thread_num
		self.filename = filename

	def run(self):
		print("Starting " + self.filename)
		Database(self.filename).run(self.thread_num)
		print("Exiting " + self.name)


def main():
	completed = read_file(log_filename)
	filenames = format_json.run()["files"]
	for filename in filenames:
		if filename in completed:
			continue
		for thread_num in range(1, maximum_thread_limit+1):
			Create_Thread(thread_num, filename).start()

		append_file(log_filename, filename)


if __name__ == "__main__":
	main()
