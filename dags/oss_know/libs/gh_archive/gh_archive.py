import datetime
import gzip
import logger
import pytz
import urllib3

http = urllib3.PoolManager(num_pools=50)


class GHArchive:
    def __init__(self):
        pass

    def get_gh_archive_by_hour(self, parent_path, url):
        if not parent_path.endswith('/'):
            parent_path = parent_path + '/'
        # 维护一个表 记录已经存入哪些小时的gha
        # year month day hour state
        # 维护一个
        req = None
        try:
            logger.info(f'download {url}')
            file_name = url.split('/')[-1]
            # logger.info(file_name)
            file_path = f"{parent_path}{file_name.split('-')[0]}/{file_name}"
            logger.info(file_path)
            date_parts = file_name.rstrip('.json.gz').split('-')
            year = int(date_parts[0])
            month = int(date_parts[1])
            day = int(date_parts[2])
            hour = int(date_parts[3])
            archive_date = datetime.datetime(year, month, day, hour)

            req = http.request('GET', url, preload_content=False, retries=3)
            chunk_size = 1024
            # 从网上块下载按照 chunk_size的大小获取数据并写入本地磁盘
            with open(file_path, 'wb') as out:
                while True:
                    data = req.read(chunk_size)
                    if not data:
                        break
                    out.write(data)
        except urllib3.exceptions.HTTPError as e:
            logger.info('HTTPError::', e)
            # failed_urls.append(url)
        except ValueError as e:
            logger.info('failed to handle value', e)
        finally:
            if req:
                req.release_conn()

    def unzip(self, parent_path, gz_filename):
        """

        :param parent_path: 解压的文件所在目录
        :param gz_filename: 解压的文件名
        :return:
        """
        try:
            if not parent_path.endswith('/'):
                parent_path = parent_path + '/'
            filename = gz_filename.replace('.gz', '')
            g_file = gzip.GzipFile(parent_path + gz_filename)

            open(f'{parent_path}{filename}', 'wb+').write(g_file.read())
            g_file.close()
        except Exception as e:
            logger.info(e)

    # 在半点时候执行
    def get_hour(self, current_date):
        results = {}
        # 从数据库中取最新的gha日期
        utc_tz = pytz.timezone('UTC')
        now_date = datetime.datetime.now(utc_tz) - datetime.timedelta(hours=1)

        while current_date.strftime('%Y-%m-%d-%-H') < now_date.strftime('%Y-%m-%d-%-H'):
            current_date = current_date + datetime.timedelta(hours=1)
            current_date_str = current_date.strftime('%Y-%m-%d-%-H')
            download_url = f'https://data.gharchive.org/{current_date_str}.json.gz'
            current_date_str_list = results.get(current_date.strftime('%Y-%m-%d'), [])
            current_date_str_list.append(current_date_str)
            results[current_date.strftime('%Y-%m-%d')] = current_date_str_list
        return results


# gha = GHArchive()
# gha.get_gh_archive_by_hour('/home/malin/gha', datetime.datetime(2021, 11, 28, 23))
# gha.unzip('/home/malin/gha/2021', '2021-11-28-23.json.gz')
# gha.get_hour()