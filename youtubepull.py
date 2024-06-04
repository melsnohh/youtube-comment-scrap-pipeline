from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import time
import pandas as pd
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import datetime
import psycopg2
import os
from dotenv import find_dotenv, load_dotenv


chrome_options = Options()
chrome_options.add_experimental_option("detach", True)
chrome_options.add_argument("headless")
chrome_options.add_argument("window-size=1920x1080")

website = "https://www.youtube.com/watch?v=RZNQyC33J8U"
path = '/Users/msnoh/Downloads/chromedriver-mac-arm64/chromedriver'
s = Service(path)

driver = webdriver.Chrome(service=s, options=chrome_options)
driver.get(website)

time.sleep(5)
last_page_height = driver.execute_script(" return window.scrollY")
comment_boxes = []

while True:
    driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
    time.sleep(.5)
    cur_height = driver.execute_script(" return window.scrollY")
    if cur_height == last_page_height:
        comment_boxes = driver.find_elements(By.XPATH, '//div[@id="primary"]/div/div[@id="below"]/'
                                                       'ytd-comments/ytd-item-section-renderer/div[@id="contents"]/'
                                                       'ytd-comment-thread-renderer/'
                                                       'ytd-comment-view-model/div[@id="body"]')
        break
    else:
        last_page_height = cur_height

today_date = datetime.date.today()
cur_date = []
comment_list = []
user_name = []
since_posted = []
total_likes = []

for comment in comment_boxes:
    user_comment = comment.find_element(By.XPATH, './/div[@id="main"]/ytd-expander/div/yt-attributed-string/span').text
    comment_likes = comment.find_element(By.XPATH, './/div[@id="main"]/ytd-comment-engagement-bar/div/span').text
    comment_user_name = comment.find_element(By.XPATH, './/div[@id="main"]/div[@id="header"]'
                                                       '/div[@id="header-author"]/h3/a/span').text
    when_posted = comment.find_element(By.XPATH, './/div[@id="main"]/div[@id="header"]/div[@id="header-author"]'
                                                 '/span[@id="published-time-text"]/a').text

    cur_date.append(today_date)
    comment_list.append(user_comment)
    user_name.append(comment_user_name)
    since_posted.append(when_posted)
    total_likes.append(comment_likes)

driver.close()


df = pd.DataFrame({
    'username': user_name,
    'comment': comment_list,
    'comment_likes': total_likes,
    'since_posted': since_posted,
    'current_date': cur_date

})

df['current_date'] = pd.to_datetime(df['current_date']).dt.strftime('%m/%d/%Y')

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

df.to_csv('youtube_vid_comments_info.csv', index=False)

conn = psycopg2.connect(host=os.getenv('host'), dbname=os.getenv('dbname'), user=os.getenv('user'),
                        password=os.getenv('password'), port=os.getenv('port'))

cur = conn.cursor()

cur.execute(
    """
        DROP TABLE IF EXISTS youtube_vid_info;

        CREATE TABLE youtube_vid_info(
            username VARCHAR,
            comment VARCHAR,
            comment_likes VARCHAR,
            since_posted VARCHAR,
            now_date VARCHAR
        );

    """
)


with open('youtube_vid_comments_info.csv', 'r') as file:
    # Notice that we don't need the csv module.
    # next(f)  # Skip the header row.
    # cur.copy_from(f, "book", sep=',')
    sql = "COPY youtube_vid_info FROM STDIN DELIMITER ',' CSV HEADER"
    cur.copy_expert(sql, file)

conn.commit()
conn.commit()


cur.close()
conn.close()



