import sqlite3
import json
from datetime import datetime
import mmap
import sys
import time
from tqdm import tqdm
from settings import *


timeframe = sys.argv[1:][0] if sys.argv[1:] else "RC_2008-01.json"
sql_transaction = []


def acceptable(data):
	# Comment contains more than 50 words or less than 1
	if len(data.split()) > 50 or len(data) < 1:
		return False
	# Comment was over 1000 characters
	if len(data) >= 1000:
		return False
	# Comment was deleted/removed
	if data in ("[deleted]", "[removed]"):
		return False
	# Comment was acceptable
	return True

def format_data(data):
	data = data \
		.replace("\n", " newlinechar ") \
		.replace("\r", "newlinechar")   \
		.replace("\"", "'")             \

	return data


class Database:
	def __init__(self, file_length, timeframe=timeframe):
		self.file_length = file_length
		self.timeframe = timeframe
		self.connection = sqlite3.connect(f"{data_drive_letter}:/REDDIT_DATA/reddit_database.db")
		self.c = self.connection.cursor()
		self.comment_id = None
		self.parent_id = None
		self.parent = None
		self.comment = None
		self.subreddit = None
		self.created_utc = None
		self.score = None

	def transaction_bldr(self, sql):
		global sql_transaction

		sql_transaction.append(sql)
		if len(sql_transaction) > 2048:
			self.c.execute("BEGIN TRANSACTION")
			for s in sql_transaction:
				try:
					self.c.execute(s)
				except Exception as e:
					# print(f"transaction_bldr: {e}")
					pass
			self.connection.commit()
			sql_transaction = []

	def create_table(self):
		self.c.execute(
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

	def find_existing_score(self, parent_id):
		try:
			sql = f"SELECT score FROM parent_reply WHERE parent_id = '{parent_id}' LIMIT 1"
			self.c.execute(sql)
			result = self.c.fetchone()

			if result:
				return result[0]
			return False
		except Exception as e:
			print(f"find_existing_score: {e}")
			return False

	def find_parent(self, parent_id):
		try:
			sql = f"SELECT comment FROM parent_reply WHERE comment_id = '{parent_id}' LIMIT 1"
			self.c.execute(sql)
			result = self.c.fetchone()

			if result is not None:
				return result[0]
			return False
		except Exception as e:
			print(f"find_parent: {e}")
			return False

	def sql_insert_no_parent(self):
		try:
			sql = f"INSERT INTO parent_reply (\
				parent_id, \
				comment_id, \
				comment, \
				subreddit, \
				unix, \
				score) VALUES (\
					\"{self.parent_id}\", \
					\"{self.comment_id}\", \
					\"{self.comment}\", \
					\"{self.subreddit}\", \
					{int(self.created_utc)}, \
					{self.score} \
			);"
			self.transaction_bldr(sql)
		except Exception as e:
			print(f"sql_insert_no_parent: {e}")

	def sql_insert_replace_comment(self):
		try:
			sql = f"UPDATE parent_reply SET \
				parent_id = {self.parent_id}, \
				comment_id = {self.comment_id}, \
				parent = {self.parent}, \
				comment = {self.comment}, \
				subreddit = {self.subreddit}, \
				unix = {int(self.created_utc)}, \
				score = {self.score} WHERE parent_id = {self.parent_id};"
			self.transaction_bldr(sql)
		except Exception as e:
			print(f"replace_comment: {e}")

	def sql_insert_has_parent(self):
		try:
			sql = f"INSERT INTO parent_reply (\
				parent_id, \
				comment_id, \
				parent, \
				comment, \
				subreddit,\
				unix, \
				score) VALUES (\
					\"{self.parent_id}\", \
					\"{self.comment_id}\", \
					\"{self.parent}\", \
					\"{self.comment}\", \
					\"{self.subreddit}\", \
					{int(self.created_utc)}, \
					{self.score} \
			);"
			self.transaction_bldr(sql)
		except Exception as e:
			print(f"sql_insert_has_parent: {e}")

	def run(self, thread_num):
		self.create_table()
		row_counter = 0
		paired_rows = 0

		file_path = f"{data_drive_letter}:/REDDIT_DATA/{self.timeframe}"
		with open(file_path, buffering=16384) as file:
			# for row in tqdm(file, desc=self.timeframe, total=self.file_length):
			for row in file:
				start_time = time.time()
				row_counter += 1
				if (row_counter+thread_num) % (maximum_thread_limit) != 0:
					print(f"Row: {row_counter}, Thread: {thread_num}, Time Taken {(time.time()-start_time)/1000}ms")
					continue
				time.sleep(0.1)
				row = json.loads(row)
				self.parent_id = row["parent_id"]
				self.comment = format_data(row["body"])
				self.created_utc = row["created_utc"]
				self.score = row["score"]
				try:
					self.comment_id = row["name"]
				except KeyError:
					self.comment_id = row["author"]
				self.subreddit = row["subreddit"]
				self.parent = self.find_parent(self.parent_id)

				if self.score > 1:
					if acceptable(self.comment):
						existing_comment_score = self.find_existing_score(self.parent_id)
						if existing_comment_score:
							if self.score > existing_comment_score:
								self.sql_insert_replace_comment()
						else:
							if self.parent:
								self.sql_insert_has_parent()
								paired_rows += 1
							else:
								self.sql_insert_no_parent()

				print(f"Row: {row_counter}, Thread: {thread_num}, Time Taken {(time.time()-start_time)/1000}ms")
		# print(f"Total rows read: {row_counter}, Paired rows: {paired_rows}, Time: {datetime.now()}")


if __name__ == "__main__":
	database = Database()
	database.run(1)
