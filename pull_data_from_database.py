import os
import cv2
from urllib.requests import urlopen
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

def save_web_image(url, idx, path='downloaded_images'):
    try:
        req = urlopen(url)
        arr = np.asarray(bytearray(req.read()), dtype=np.uint8)
        img = cv2.imdecode(arr, -1)

        base_name = url.split('?')[0].split('/')[-1]
        cv2.imwrite("{}/{}".format(path, base_name), img)
    except:
        print(url)

def paralell_proc(dl_list, workers):
    with tqdm(total=len(dl_list), desc="In Progress") as pbar:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(save_web_image, d[0], idx) for idx, d in enumerate(dl_list)]
            for futures in as_completed(futures):
                result = future.result()
                pbar.update(1)

host = 'host-name'
port = 80
database = 'database-name'
user = 'user-name'
password = 'password'

conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
cur = conn.cursor()

sql = '''
    select
        xxx, xxx
    from
        xxx
    where
        xxx
'''

cur.execute(sql)
data = cur.fetchall()

conn.commit()
cur.close()
conn.close()

paralell_proc(data, 16)

