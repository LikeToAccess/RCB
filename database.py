import sqlite3
import json
from datetime import datetime
import mmap
from tqdm import tqdm
from settings import *


timeframe = "RC_2008-01"
sql_transaction = []

connection = sqlite3.connect(f"{data_drive_letter}:/REDDIT_DATA/{timeframe}.db")
c = connection.cursor()



def get_num_lines(file_path, memory_mapped=True):
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

def transaction_bldr(sql):
	global sql_transaction

	sql_transaction.append(sql)
	if len(sql_transaction) > 2048:
		c.execute("BEGIN TRANSACTION")
		for s in sql_transaction:
			try:
				c.execute(s)
			except Exception as e:
				# print(f"transaction_bldr: {e}")
				pass
		connection.commit()
		sql_transaction = []

def sql_insert_replace_comment(comment_id, parent_id, parent, comment, subreddit, time, score):
	try:
		sql = f"UPDATE parent_reply SET \
			parent_id = {parent_id}, \
			comment_id = {comment_id}, \
			parent = {parent}, \
			comment = {comment}, \
			subreddit = {subreddit}, \
			unix = {int(time)}, \
			score = {score} WHERE parent_id = {parent_id};"
		transaction_bldr(sql)
	except Exception as e:
		print(f"replace_comment: {e}")

def sql_insert_has_parent(comment_id, parent_id, parent, comment, subreddit, time, score):
	try:
		sql = f"INSERT INTO parent_reply (\
			parent_id, \
			comment_id, \
			parent, \
			comment, \
			subreddit,\
			unix, \
			score) VALUES (\
				\"{parent_id}\", \
				\"{comment_id}\", \
				\"{parent}\", \
				\"{comment}\", \
				\"{subreddit}\", \
				{int(time)}, \
				{score} \
		);"
		transaction_bldr(sql)
	except Exception as e:
		print(f"sql_insert_has_parent: {e}")

def sql_insert_no_parent(comment_id, parent_id, comment, subreddit, time, score):
	try:
		sql = f"INSERT INTO parent_reply (\
			parent_id, \
			comment_id, \
			comment, \
			subreddit, \
			unix, \
			score) VALUES (\
				\"{parent_id}\", \
				\"{comment_id}\", \
				\"{comment}\", \
				\"{subreddit}\", \
				{int(time)}, \
				{score} \
		);"
		transaction_bldr(sql)
	except Exception as e:
		print(f"sql_insert_no_parent: {e}")


def acceptable(data):
	# Comment contains more than 50 words or less than 1
	if len(data.split()) > 50 or len(data) < 1:
		return False
	# Comment was over 1000 characters
	elif len(data) >= 1000:
		return False
	# Comment was deleted/removed
	elif data == "[deleted]" or data == "[removed]":
		return False
	# Comment was acceptable
	else:
		return True

def find_existing_score(parent_id):
	try:
		sql = f"SELECT score FROM parent_reply WHERE parent_id = '{parent_id}' LIMIT 1"
		c.execute(sql)
		result = c.fetchone()

		if result:
			return result[0]
		return False
	except Exception as e:
		print(f"find_existing_score: {e}")
		return False

def find_parent(parent_id):
	try:
		sql = f"SELECT comment FROM parent_reply WHERE comment_id = '{parent_id}' LIMIT 1"
		c.execute(sql)
		result = c.fetchone()

		if result is not None:
			return result[0]
		return False
	except Exception as e:
		print(f"find_parent: {e}")
		return False

def format_data(data):
	data = data \
		.replace("\n", " newlinechar ") \
		.replace("\r", "newlinechar")   \
		.replace("\"", "'")             \

	return data

def create_table():
	c.execute(
		"CREATE TABLE IF NOT EXISTS parent_reply(\
			parent_id TEXT PRIMARY KEY, \
			comment_id TEXT UNIQUE, \
			parent TEXT, \
			comment TEXT, \
			subreddit TEXT, \
			unix INT, \
			score INT \
		)"
	)

def main():
	create_table()
	row_counter = 0
	paired_rows = 0

	file_path = f"{data_drive_letter}:/REDDIT_DATA/{timeframe}"
	with open(file_path, buffering=16384) as file:
		# for row in file:
		for row in tqdm(file, total=get_num_lines(file_path)):
			row_counter += 1
			row = json.loads(row)
			parent_id = row["parent_id"]
			body = format_data(row["body"])
			created_utc = row["created_utc"]
			score = row["score"]
			try:
				comment_id = row["name"]
			except KeyError:
				comment_id = row["author"]
			subreddit = row["subreddit"]
			parent_data = find_parent(parent_id)

			if score > 1:
				if acceptable(body):
					existing_comment_score = find_existing_score(parent_id)
					if existing_comment_score:
						if score > existing_comment_score:
							sql_insert_replace_comment(
								comment_id,
								parent_id,
								parent_data,
								body,
								subreddit,
								created_utc,
								score
							)
					else:
						if parent_data:
							sql_insert_has_parent(comment_id,
								parent_id,
								parent_data,
								body,
								subreddit,
								created_utc,
								score
							)
							paired_rows += 1
						else:
							sql_insert_no_parent(
								comment_id,
								parent_id,
								body,
								subreddit,
								created_utc,
								score
							)

			# if row_counter % 100000 == 0:
				# print(f"Total rows read: {row_counter}, Paired rows: {paired_rows}, Time: {datetime.now()}")


if __name__ == "__main__":
	main()
