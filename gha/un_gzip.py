# -*-coding:utf-8-*-
import gzip
import calendar
def un_gzip(gz_filename):
    try:

        filename = gz_filename.replace('.gz', '')
        # print(filename)
        # open(f'{filename}', 'wb+')
        g_file = gzip.GzipFile(gz_filename)
        open(f'{filename}', 'wb+').write(g_file.read())
        g_file.close()
    except:
        print(gz_filename)


for year in range(2019, 2020):
    count = 0
    for month in range(1, 13):
        day_count = calendar.monthrange(year, month)[1]
        if month < 10:
            month = '0' + str(month)
        for i in range(1, day_count + 1):
            if i < 10:
                i = '0' + str(i)
            for j in range(24):
                # print(f"{year}-{month}-{i}-{j}.json.gz")
                un_gzip(f"{year}-{month}-{i}-{j}.json.gz")
                # count+=1
    # print(count)

for year in range(2019, 2020):
    count = 0
    for month in range(1, 13):
        day_count = calendar.monthrange(year, month)[1]
        if month < 10:
            month = '0' + str(month)
        for i in range(1, day_count + 1):
            if i < 10:
                i = '0' + str(i)
            for j in range(24):
                # print(f"{year}-{month}-{i}-{j}.json.gz")
                open(f"{year}-{month}-{i}-{j}.json.gz",'r')