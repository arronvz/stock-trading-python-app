import schedule
import time
from script import run_stock_job

from datetime import datetime

def basic_job():
    print("Basic Job started at:" , datetime.now())

def scheduled_stock_job():
    print("Stock job triggered at:", datetime.now())
    try:
        run_stock_job()
        print("Stock job completed successfully")
    except Exception as e:
        print("Stock job failed:", e)

schedule.every(1).minute.do(basic_job)
schedule.every(1).minute.do(run_stock_job)

# schedule.every().day.at("08:03").do(scheduled_stock_job)

print("Scheduler started...")

while True:
    schedule.run_pending()
    time.sleep(1)