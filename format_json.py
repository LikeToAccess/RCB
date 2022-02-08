import os
from settings import data_drive_letter


def run():
	os.chdir(f"{data_drive_letter}:/REDDIT_DATA")
	files_read = []
	files_changed = []

	for filename in os.listdir():
		if "RC_" in filename:
			if filename.endswith(".json"):
				files_read.append(filename)
			else:
				try:
					os.rename(filename, f"{filename}.json")
				except FileExistsError:
					os.remove(filename)

				files_read.append(filename)
				files_changed.append(filename)

	print(f"{len(files_changed)}/{len(files_read)} (total) files changed.")
	return {"files":files_read, "new_files":files_changed}


if __name__ == "__main__":
	run()
