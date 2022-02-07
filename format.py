import os
from settings import data_drive_letter


def main():
	os.chdir(f"{data_drive_letter}:/REDDIT_DATA")
	files_changed = 0
	for files_read, filename in enumerate(os.listdir()):
		if "RC_" in filename and ".json" not in filename:
			try:
				os.rename(filename, f"{filename}.json")
			except FileExistsError:
				os.remove(filename)
			files_changed += 1

	print(f"{files_changed}/{files_read} (total) files changed.")


if __name__ == "__main__":
	main()
