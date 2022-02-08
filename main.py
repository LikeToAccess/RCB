import format_json
from database import Database


def main():
	filenames = format_json.run()["files"]
	for filename in filenames:
		Database.run(filename)


if __name__ == "__main__":
	main()
