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
        "ALTER TABLE Eworldtours_AllOrders ALTER COLUMN AmountPaid NUMERIC(16, 4)",
        "ALTER TABLE Eworldtours_AllOrders ALTER COLUMN AmountUnpaid NUMERIC(16, 4)",
        "ALTER TABLE Eworldtours_AllOrders ALTER COLUMN Rebate NUMERIC(16, 4)",
        "ALTER TABLE Eworldtours_AllOrders ALTER COLUMN ProportionalRebate NUMERIC(16, 4)",
        "ALTER TABLE Eworldtours_AllOrders ALTER COLUMN PerCapitaRebate NUMERIC(16, 4)",
        "ALTER TABLE Eworldtours_AllOrders ALTER COLUMN AmountReceivable NUMERIC(16, 4)",
        "ALTER TABLE Eworldtours_AllOrders ALTER COLUMN AmountReceived NUMERIC(16, 4)",
        "ALTER TABLE Eworldtours_AllOrders ALTER COLUMN AmountUnreceived NUMERIC(16, 4)",
        "ALTER TABLE Eworldtours_AllOrders ALTER COLUMN LossCost NUMERIC(16, 4)",
        "ALTER TABLE Eworldtours_AllOrders ALTER COLUMN SupplierRetainedLoss NUMERIC(16, 4)",
        "ALTER TABLE Eworldtours_AllOrders ALTER COLUMN GrossProfit NUMERIC(16, 4)",
        "ALTER TABLE Eworldtours_AllOrders ALTER COLUMN ContractAmount NUMERIC(16, 4)",
        "ALTER TABLE Eworldtours_AllOrders ALTER COLUMN Tax NUMERIC(16, 4)",
        "ALTER TABLE Eworldtours_AllOrders ALTER COLUMN BranchRebates NUMERIC(16, 4)",
        "ALTER TABLE Eworldtours_AllOrders ALTER COLUMN Promotions NUMERIC(16, 4)"
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