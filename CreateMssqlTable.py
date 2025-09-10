import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# 加载环境变量
load_dotenv(override=True)

# 连接数据库
DB_PASSWORD = os.getenv('AZURE_SQL_PASSWORD')
connection_string = "mysql+pymysql://root:root@localhost/CruiseCrawlerDB?charset=utf8mb4"

engine = create_engine(
    connection_string,
    isolation_level="REPEATABLE READ",  # 防止幻读和不可重复读
    pool_size=20,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=600
)

# 使用引擎连接数据库
with engine.connect() as connection:
    try:
        # 定义要删除的表名列表
        tables_to_drop = [
            "Eworldtours_AllOrders",
            # 可以添加其他需要删除的表名
        ]

        for table in tables_to_drop:
            # 使用 text() 包装 SQL 语句，并使用 IF EXISTS 避免表不存在时出错[2,3](@ref)
            drop_query = text(f"DROP TABLE IF EXISTS {table}")
            connection.execute(drop_query)
            print(f"已执行: {drop_query}")

        # 然后执行多个ALTER TABLE语句来修改列类型
        alter_queries = [
            "ALTER TABLE Eworldtours_AllOrders ALTER COLUMN AmountPayable NUMERIC(16, 4)",
            # ... 你的其他 ALTER 语句
            "ALTER TABLE Eworldtours_AllOrders ALTER COLUMN Promotions NUMERIC(16, 4)"
        ]



        print("所有操作已成功执行")

    except Exception as e:
        # 发生错误时回滚
        print(f"数据库操作出错: {e}")
        connection.rollback()
    finally:
        # 连接会在 with 块结束后自动关闭
        print("数据库连接已关闭")