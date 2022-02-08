import format_json
from database import Database


log_filename = "completed.log"


def read_file(filename):
	try:
		with open(filename, "r") as file:
			data = file.read().split("\n")
	except FileExistsError:
		write_file(filename, "")
		return read_file(filename)
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
		Database(filename).run()
		append_file(log_filename, filename)


if __name__ == "__main__":
	main()
