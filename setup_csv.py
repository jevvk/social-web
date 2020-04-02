import csv
import sqlite3

DATABASE_FILE = 'data/datasets.db'
KAGGLE_DATA = 'data/kaggle_linkedin_dataset.csv'
FACEBOOK_DATA = 'data/facebook_dataset.csv'
LINKEDIN_DATA = 'data/linkedin_dataset.csv'

KAGGLE_TABLE = '''
  CREATE TABLE `kaggle_dataset` (
  `ageEstimate` double DEFAULT NULL,
  `companyFollowerCount` double DEFAULT NULL,
  `companyHasLogo` text,
  `companyName` text,
  `companyStaffCount` double DEFAULT NULL,
  `companyUrl` text,
  `companyUrn` text,
  `connectionsCount` double DEFAULT NULL,
  `country` text,
  `endDate` text,
  `followable` double DEFAULT NULL,
  `followersCount` double DEFAULT NULL,
  `genderEstimate` text,
  `hasPicture` text,
  `isPremium` double DEFAULT NULL,
  `mbrLocation` text,
  `mbrLocationCode` text,
  `mbrTitle` text,
  `memberUrn` text,
  `posLocation` text,
  `posLocationCode` text,
  `posTitle` text,
  `positionId` double DEFAULT NULL,
  `startDate` text,
  `avgMemberPosDuration` double DEFAULT NULL,
  `avgCompanyPosDuration` double DEFAULT NULL
  )
'''

LINKEDIN_TABLE = '''
  CREATE TABLE `linkedin_dataset` (
  `prime_key` int DEFAULT NULL,
  `dataset_id` int DEFAULT NULL,
  `as_of_date` text,
  `company_name` text,
  `followers_count` int DEFAULT NULL,
  `employees_on_platform` int DEFAULT NULL,
  `link` text,
  `industry` text,
  `date_added` text,
  `date_update` text,
  `description` text,
  `website` text,
  `entity_id` text,
  `cusip` text,
  `isin` text
  )
'''

FACEBOOK_TABLE = '''
  CREATE TABLE `facebook_dataset` (
  `prime_key` int DEFAULT NULL,
  `dataset_id` int DEFAULT NULL,
  `time` text,
  `username` text,
  `checkins` int DEFAULT NULL,
  `has_added_app` text,
  `were_here_count` int DEFAULT NULL,
  `likes` int DEFAULT NULL,
  `talking_about_count` int DEFAULT NULL,
  `facebook_id` bigint DEFAULT NULL,
  `date_added` text,
  `date_updated` text,
  `entity_id` text,
  `cusip` text,
  `isin` text
  )
'''

FOLLOWERS_VS_LIKES = '''
  CREATE TABLE `follwers_vs_likes` (
  `company_name` text,
  `followers_count` int DEFAULT NULL,
  `likes` int DEFAULT NULL
  )
'''

def import_csv(csv_file, insert_row):
  with open(csv_file, 'rt', encoding='utf8') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')

    for row in reader:
      insert_row(row)

if __name__ == "__main__":
  conn = sqlite3.connect(DATABASE_FILE)
  cur = conn.cursor()

  # reset database
  tables = list(cur.execute("select name from sqlite_master where type is 'table'"))
  cur.executescript(';'.join(["drop table if exists %s" %i for i in tables]))

  cur.executescript(KAGGLE_TABLE)
  print('Created kaggle table.')

  cur.executescript(LINKEDIN_TABLE)
  print('Created linkedin table.')

  cur.executescript(FACEBOOK_TABLE)
  print('Created facebook table.')

  def kaggle_insert(row):
    for key in row.keys():
      if key is None or row[key] is None: continue
      row[key] = row[key].replace('"', '""')

    cur.execute(f'insert into kaggle_dataset values (' +
      f'"{row["ageEstimate"]}", "{row["companyFollowerCount"]}", "{row["companyHasLogo"]}", "{row["companyName"]}", "{row["companyStaffCount"]}",' +
      f'"{row["companyUrl"]}", "{row["companyUrn"]}", "{row["connectionsCount"]}", "{row["country"]}", "{row["endDate"]}",' +
      f'"{row["followable"]}", "{row["followersCount"]}", "{row["genderEstimate"]}", "{row["hasPicture"]}", "{row["isPremium"]}",' +
      f'"{row["mbrLocation"]}", "{row["mbrLocationCode"]}", "{row["mbrTitle"]}", "{row["memberUrn"]}", "{row["posLocation"]}",' +
      f'"{row["posLocationCode"]}", "{row["posTitle"]}", "{row["positionId"]}", "{row["startDate"]}", "{row["avgMemberPosDuration"]}",' +
      f'"{row["avgCompanyPosDuration"]}")'
    )

  import_csv(KAGGLE_DATA, kaggle_insert)
  print('Imported kaggle data.')

  def linkedin_insert(row):
    for key in row.keys():
      if key is None or row[key] is None: continue
      row[key] = row[key].replace('"', '""')

    description = row["description"].replace("\n", "\\n") if row["description"] else None

    cur.execute(f'insert into linkedin_dataset values (' +
      f'"{row["prime_key"]}", "{row["dataset_id"]}", "{row["as_of_date"]}", "{row["company_name"]}", "{row["followers_count"]}",' +
      f'"{row["employees_on_platform"]}", "{row["link"]}", "{row["industry"]}", "{row["date_added"]}", "{row["date_update"]}",' +
      f'"{description}", "{row["website"]}", "{row["entity_id"]}", "{row["cusip"]}", "{row["isin"]}")'
    )

  import_csv(LINKEDIN_DATA, linkedin_insert)
  print('Imported linkedin data.')

  def facebook_insert(row):
    for key in row.keys():
      if key is None or row[key] is None: continue
      row[key] = row[key].replace('"', '""')

    cur.execute(f'insert into facebook_dataset values (' +
      f'"{row["prime_key"]}", "{row["dataset_id"]}", "{row["time"]}", "{row["username"]}", "{row["checkins"]}",' +
      f'"{row["has_added_app"]}", "{row["were_here_count"]}", "{row["likes"]}", "{row["talking_about_count"]}", "{row["facebook_id"]}",' +
      f'"{row["date_added"]}", "{row["date_updated"]}", "{row["entity_id"]}", "{row["cusip"]}", "{row["isin"]}")'
    )

  import_csv(FACEBOOK_DATA, facebook_insert)
  print('Imported facebook data.')

  conn.commit()
  conn.close()
