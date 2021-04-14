from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import time
import timeit
from datetime import date


with open("../db_info.txt") as f:
    lines = f.readlines()
    db_id = lines[0].strip()
    db_pwd = lines[1].strip()

db_ip = 'localhost'
db_port = '3306'

class Engine:

    def __init__(self, db_ip = db_ip, db_port = db_port, db_id = db_id, db_pwd = db_pwd):
        self.db_ip = db_ip
        self.db_port = db_port
        self.db_id = db_id
        self.db_pwd = db_pwd

    def connect(self, db_name):
        conn = create_engine(
            "mysql+mysqldb://" + self.db_id + ":" + self.db_pwd + "@" + self.db_ip + ":" + self.db_port + "/" + db_name,
            encoding='utf-8')

        return conn