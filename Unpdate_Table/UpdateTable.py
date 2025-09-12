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
    f'PWD=EC90-18AC15y&00FE8086*4B286e3901;'  # 使用环境变量更安全
    'Encrypt=yes;'
    'TrustServerCertificate=no;'
    'Connection Timeout=30;'
)
cursor = connection.cursor()

try:
    # 定义要更新的ProductId列表
    product_ids = [
        'FRSK2406110009', 'FRSK2406110006', 'FRSK2406110005',
        'FRSK2406110004', 'FRSK2406110003', 'FRSK2406110002',
        'FRSK2406110001', 'FRSK2406100002', 'FRSK2406100001',
        'FRSK2406070001', 'FRSK2406060002'
    ]
    add_col = "ALTER TABLE Eworldtours_AllOrders ADD ERP_Type VARCHAR(50);"
    changeName = "EXEC sp_rename 'Eworldtours_AllOrders.ERP_Type', 'AccountName', 'COLUMN';"
    # 使用参数化查询批量更新[1,5](@ref)
    #update_query = "UPDATE Eworldtours_AllOrders SET OrderType = ? WHERE OrderId = ?"
    # 准备数据：为每个ProductId创建一个元组(新值, ProductId)
    #update_data = [('SupplyOrder-Tinma', pid) for pid in product_ids]
    cursor.execute(changeName)
    # 使用executemany批量执行更新
    #cursor.executemany(update_query, update_data)

    connection.commit()
    #print(f"成功更新了 {len(product_ids)} 条记录的OrderType字段")
    print(f"成功更新了 ERP_Type 字段")
except pyodbc.Error as e:
    print(f"数据库操作出错: {e}")
    connection.rollback()
finally:
    cursor.close()
    connection.close()
    print("数据库连接已关闭")