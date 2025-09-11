import pyodbc
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv(override=True)

# 连接数据库
DB_PASSWORD = os.getenv('AZURE_SQL_PASSWORD')
connection = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=gtt-azure-group.database.windows.net;'
    'DATABASE=CruiseCrawlerDB;'
    'UID=gttadmin;'
    f'PWD=EC90-18AC15y&00FE8086*4B286e3901;'  # 建议使用环境变量
    'Encrypt=yes;'
    'TrustServerCertificate=no;'
    'Connection Timeout=30;'
)
cursor = connection.cursor()

try:
    # 执行多个ALTER TABLE语句来修改列类型
    alter_queries = [
        "ALTER TABLE Eworldtours_AllOrders ALTER COLUMN AmountPayable NUMERIC(16, 4)",

    ]

    for query in alter_queries:
        cursor.execute(query)
        print(f"已执行: {query}")

    connection.commit()
    print("所有字段类型已成功修改为NUMERIC(16, 4)")

except pyodbc.Error as e:
    print(f"数据库操作出错: {e}")
    connection.rollback()
finally:
    cursor.close()
    connection.close()
    print("数据库连接已关闭")