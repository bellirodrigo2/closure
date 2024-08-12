import random
from string import Template

from db import bootstrap

q_last_day = "SELECT * FROM stocks_real_time WHERE time > now() - INTERVAL '1 day';"
q_last_ten = "SELECT * FROM stocks_real_time ORDER BY time DESC, price DESC LIMIT 10;"

q_apple_last_2days = """SELECT avg(price) FROM stocks_real_time srt JOIN company c ON c.symbol = srt.symbol 
    WHERE c.name = 'Apple' AND time > '2024-07-05 00:00:00+00:00';"""  # now() - INTERVAL '2 days';"""

q_first_last = """SELECT symbol, first(price, time), last(price, time) FROM stocks_real_time srt
    WHERE time > now() - INTERVAL '45 days' GROUP BY symbol;"""

q_day_avg = """SELECT
    time_bucket('1 day', time) AS bucket,
    symbol,
    avg(price)
FROM stocks_real_time srt WHERE time > now() - INTERVAL '6 week'
GROUP BY bucket, symbol
ORDER BY bucket, symbol;"""


with bootstrap() as conn:
    cur = conn.cursor(name="cursor1")

    # cur.execute(q_last_day)
    # cur.execute(q_last_ten)
    # cur.execute(q_apple_last_2days)
    # cur.execute(q_first_last)
    # cur.execute(q_day_avg)

    # for row in cur.fetchall():
    # print(row)
    table = "stocks_real_time"
    cols = ["time", "symbol", "price"]
    # cols_select = ["time_bucket('1 day', time) AS bucket", "symbol", "avg(price)"]
    cols_select = None
    sch = "public"

    cur.execute(
        "SELECT get_dynamic_select2(%s, %s, %s)",
        (table, cols, cols_select),
    )
    select = cur.fetchone()[0]

    starttime = "time > now() - INTERVAL '2 day'"
    endtime = "AND time < now() - INTERVAL '1 day'"

    querybegin = f"{select} WHERE {starttime} {endtime} "
    queryend = f" GROUP BY bucket, symbol;" if cols_select else ";"
    query_stmt = querybegin + queryend

    STMT = Template("SELECT $cols FROM $table $join $where $groupby $orderby $limit;")

    # cur.execute(query_stmt)

    # # Fetch rows in batches
    # batch_size = 1000
    # counter = 0
    # while True:
    #     rows = cur.fetchmany(batch_size)
    #     if not rows:
    #         break

    #     for row in rows:
    #         print(row)  # Process each row
    #         counter += 1
    # print(counter)


import csv

input_file = "ti.csv"
output_file = "tio.csv"

with open(input_file, "r") as infile, open(output_file, "w", newline="") as outfile:
    reader = csv.reader(infile, delimiter=";")
    writer = csv.writer(outfile, delimiter=";")
    for row in reader:
        for n in range(1, 4):
            try:
                float(row[n])
            except ValueError:
                row[n] = random.random()
        # writer.writerow(row)
