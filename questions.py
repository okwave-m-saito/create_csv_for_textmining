import os
import csv
import time

from os.path import join, dirname

import pymysql.cursors

from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

MYSQL_USER = os.environ.get('MYSQL_USER')
MYSQL_PASSWD = os.environ.get('MYSQL_PASSWD')
MYSQL_DB = os.environ.get('MYSQL_DB')
MYSQL_HOST = os.environ.get('MYSQL_HOST')
MYSQL_PORT = os.environ.get('MYSQL_PORT')
PATH_QUESTION_IDS = os.environ.get('PATH_QUESTION_IDS')
PATH_RESULT = os.environ.get('PATH_RESULT')

question_ids = open(PATH_QUESTION_IDS, 'r', encoding='utf-8')
list_in = []
for row in question_ids:
    list_in.append(row.rstrip('\n'))
str_in = ','.join(list_in)
question_ids.close()

conn = pymysql.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWD,
    port=int(MYSQL_PORT),
    db=MYSQL_DB,
    charset='ujis',
    cursorclass=pymysql.cursors.DictCursor)

try:
    with conn.cursor() as cursor:
        sql = '''
SELECT
  IF(questions.question_id > 0, 'Q', 'error') AS QAflag,
  questions.user_id AS user_id,
  questions.question_id AS id,
  categories.category_string,
  questions.question_datetime AS datetime,
  CONCAT(YEAR(questions.question_datetime),'年',MONTH(questions.question_datetime),'月') AS year1,
  DATE_FORMAT(questions.question_datetime, '%Y年') AS year2,
  IF(MONTH(questions.question_datetime) IN (1,2,3), CONCAT(YEAR(questions.question_datetime) - 1, '年度'), CONCAT(YEAR(questions.question_datetime), '年度')) AS fiscalyear,
  CONCAT(MONTH(questions.question_datetime), '月') AS month,
  DAY(questions.question_datetime) AS day,
  DAYNAME(questions.question_datetime) AS dayname,
  HOUR(questions.question_datetime) AS hour,
  IF(MONTH(questions.question_datetime) IN (1,2,3), '冬', IF(MONTH(questions.question_datetime) IN (4,5,6), '春', IF(MONTH(questions.question_datetime) IN (7,8,9), '夏', IF(MONTH(questions.question_datetime) IN (10,11,12), '秋','')))) AS season,
  DATE_FORMAT(questions.question_datetime, '1900年%m月') AS tsv,
  IF(user_gender.profile_value = '1', '男性', IF(user_gender.profile_value = '2', '女性', '不明')) AS gender,
  user_age.age AS age,
  IF(user_age.age < 0, -1, CONCAT(TRUNCATE(user_age.age, -1), '代')) AS ages,
  IF(ISNULL(user_province.profile_value) OR TRIM(user_province.profile_value) = '' OR ISNULL(province.name), '不明', province.name) AS province,
  IF(ISNULL(user_province.profile_value) OR TRIM(user_province.profile_value) = '' OR ISNULL(province.region), '不明', province.region) AS region,
  IF(ISNULL(user_occupation.profile_value) OR TRIM(user_occupation.profile_value) = '' OR ISNULL(occupation.name), '不明', occupation.name) AS occupation,
  CONCAT('"', REPLACE(REPLACE(REPLACE(questions.question_title, '"', ''), '\t', '    '), '\0', ''), '"') AS title,
  CONCAT('"', REPLACE(REPLACE(REPLACE(questions.question_text, '"', ''), '\t', '    '), '\0', ''), '"') AS text, 
  '-' AS bestanswer,
  questions.question_id AS incident_id
FROM 
  questions
  LEFT JOIN categories ON (questions.category_id = categories.category_id)
  LEFT JOIN user_profile AS user_gender ON (questions.user_id = user_gender.user_id AND user_gender.profile_key = 3)
  LEFT JOIN user_profile AS user_province ON (questions.user_id =user_province.user_id AND user_province.profile_key = 7)
  LEFT JOIN (
  SELECT 0 AS id, '北海道' AS name,'北海道地方' AS region UNION ALL
  SELECT 1 AS id, '青森県' AS name,'東北地方' AS region UNION ALL
  SELECT 2 AS id, '岩手県' AS name,'東北地方' AS region UNION ALL
  SELECT 3 AS id, '秋田県' AS name,'東北地方' AS region UNION ALL
  SELECT 4 AS id, '宮城県' AS name,'東北地方' AS region UNION ALL
  SELECT 5 AS id, '山形県' AS name,'東北地方' AS region UNION ALL
  SELECT 6 AS id, '福島県' AS name,'東北地方' AS region UNION ALL
  SELECT 7 AS id, '茨城県' AS name,'関東地方' AS region UNION ALL
  SELECT 8 AS id, '栃木県' AS name,'関東地方' AS region UNION ALL
  SELECT 9 AS id, '千葉県' AS name,'関東地方' AS region UNION ALL
  SELECT 10 AS id, '埼玉県' AS name,'関東地方' AS region UNION ALL
  SELECT 11 AS id, '東京都' AS name,'関東地方' AS region UNION ALL
  SELECT 12 AS id, '神奈川県' AS name,'関東地方' AS region UNION ALL
  SELECT 13 AS id, '群馬県' AS name,'関東地方' AS region UNION ALL
  SELECT 14 AS id, '新潟県' AS name,'中部地方' AS region UNION ALL
  SELECT 15 AS id, '長野県' AS name,'中部地方' AS region UNION ALL
  SELECT 16 AS id, '岐阜県' AS name,'中部地方' AS region UNION ALL
  SELECT 17 AS id, '山梨県' AS name,'中部地方' AS region UNION ALL
  SELECT 18 AS id, '静岡県' AS name,'中部地方' AS region UNION ALL
  SELECT 19 AS id, '愛知県' AS name,'中部地方' AS region UNION ALL
  SELECT 20 AS id, '富山県' AS name,'中部地方' AS region UNION ALL
  SELECT 21 AS id, '石川県' AS name,'中部地方' AS region UNION ALL
  SELECT 22 AS id, '福井県' AS name,'中部地方' AS region UNION ALL
  SELECT 23 AS id, '京都府' AS name,'近畿地方' AS region UNION ALL
  SELECT 24 AS id, '滋賀県' AS name,'近畿地方' AS region UNION ALL
  SELECT 25 AS id, '三重県' AS name,'近畿地方' AS region UNION ALL
  SELECT 26 AS id, '奈良県' AS name,'近畿地方' AS region UNION ALL
  SELECT 27 AS id, '和歌山県' AS name,'近畿地方' AS region UNION ALL
  SELECT 28 AS id, '大阪府' AS name,'近畿地方' AS region UNION ALL
  SELECT 29 AS id, '兵庫県' AS name,'近畿地方' AS region UNION ALL
  SELECT 30 AS id, '鳥取県' AS name,'中国地方' AS region UNION ALL
  SELECT 31 AS id, '島根県' AS name,'中国地方' AS region UNION ALL
  SELECT 32 AS id, '岡山県' AS name,'中国地方' AS region UNION ALL
  SELECT 33 AS id, '広島県' AS name,'中国地方' AS region UNION ALL
  SELECT 34 AS id, '山口県' AS name,'中国地方' AS region UNION ALL
  SELECT 35 AS id, '徳島県' AS name,'四国地方' AS region UNION ALL
  SELECT 36 AS id, '香川県' AS name,'四国地方' AS region UNION ALL
  SELECT 37 AS id, '愛媛県' AS name,'四国地方' AS region UNION ALL
  SELECT 38 AS id, '高知県' AS name,'四国地方' AS region UNION ALL
  SELECT 39 AS id, '福岡県' AS name,'九州地方' AS region UNION ALL
  SELECT 40 AS id, '大分県' AS name,'九州地方' AS region UNION ALL
  SELECT 41 AS id, '佐賀県' AS name,'九州地方' AS region UNION ALL
  SELECT 42 AS id, '長崎県' AS name,'九州地方' AS region UNION ALL
  SELECT 43 AS id, '熊本県' AS name,'九州地方' AS region UNION ALL
  SELECT 44 AS id, '宮崎県' AS name,'九州地方' AS region UNION ALL
  SELECT 45 AS id, '鹿児島県' AS name,'九州地方' AS region UNION ALL
  SELECT 46 AS id, '沖縄県' AS name,'九州地方' AS region UNION ALL
  SELECT 47 AS id, 'サンフランシスコ' AS name,'海外' AS region UNION ALL
  SELECT 48 AS id, 'ロサンゼルス' AS name,'海外' AS region UNION ALL
  SELECT 49 AS id, 'ニューヨーク' AS name,'海外' AS region UNION ALL
  SELECT 50 AS id, 'その他米国' AS name,'海外' AS region UNION ALL
  SELECT 51 AS id, 'シンガポール' AS name,'海外' AS region UNION ALL
  SELECT 52 AS id, 'その他海外' AS name,'海外' AS region  
  ) AS province ON (CAST(user_province.profile_value AS SIGNED) = province.id)
  LEFT JOIN user_profile AS user_occupation ON (questions.user_id = user_occupation.user_id AND user_occupation.profile_key = 5)
  LEFT JOIN (
  SELECT 1 AS id, '公務員' AS name UNION ALL
  SELECT 2 AS id, '会社員' AS name UNION ALL
  SELECT 3 AS id, '自営業' AS name UNION ALL
  SELECT 4 AS id, 'フリーター' AS name UNION ALL
  SELECT 5 AS id, '学生' AS name UNION ALL
  SELECT 6 AS id, '主婦' AS name UNION ALL
  SELECT 7 AS id, '無職' AS name
  ) AS occupation ON (CAST(user_occupation.profile_value AS SIGNED) = occupation.id)
  LEFT JOIN (
    SELECT
      questions.question_id,
      IF(ISNULL(user_birthdate.profile_value) OR user_birthdate.profile_value < '1800-01-01' OR user_birthdate.profile_value > questions.question_datetime, -1, YEAR(questions.question_datetime) - YEAR(user_birthdate.profile_value) - IF(DAYOFYEAR(questions.question_datetime) < DAYOFYEAR(user_birthdate.profile_value), 1, 0)) AS age
    FROM questions
    LEFT JOIN user_profile AS user_birthdate ON (questions.user_id = user_birthdate.user_id AND user_birthdate.profile_key = 4)
    WHERE
      questions.question_id IN ({0})
  ) AS user_age ON (questions.question_id = user_age.question_id)
WHERE
  questions.question_id IN ({0})
        '''.format(str_in)
        start = time.time()
        cursor.execute(sql)
        elapsed_time = time.time() - start
        print ('elapsed_time:{0}'.format(elapsed_time) + '[sec]')
        res_cur = cursor.fetchall()
finally:
    conn.close()

f = open(PATH_RESULT, 'w', encoding='shift_jis')
for row in res_cur:	
	qaflag= row['QAflag']
	user_id= row['user_id']
	question_id= row['id']
	category_string= row['category_string']
	datetime= row['datetime']
	year1= row['year1']
	year2= row['year2']
	fiscalyear= row['fiscalyear']
	month= row['month']
	day= row['day']
	dayname= row['dayname']
	hour= row['hour']
	season= row['season']
	tsv= row['tsv']
	gender= row['gender']
	age= row['age']
	ages= row['ages']
	province= row['province']
	region= row['region']
	occupation= row['occupation']
	question_title= row['title']
	question_text= row['text']
	bestanswer= row['bestanswer']
	incident_id= row['incident_id']
	
	f.write('{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23}'.format(
		qaflag,	
		user_id,
		question_id,	
		category_string,
		datetime,
		year1,
		year2,
		fiscalyear,	
		month,
		day,
		dayname,
		hour,
		season,
		tsv,
		gender,
		age,
		ages,
		province,
		region,
		occupation,
		question_title,
		question_text,
		bestanswer,
		incident_id)
	)
	f.write('\n')
f.close()
