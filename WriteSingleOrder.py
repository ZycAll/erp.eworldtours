import os

import pandas as pd
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# 假设这些导入是你项目中已有的
from Create_table import MSSQLDatabaseWriter
from order_model import Order


class ReadExcelSingleFile:
    def __init__(self):
        self.data = None
        self.filename = "AllOrder-Tinma.xlsx"  # 专注于单个文件
        self.session = None
        self.lock = threading.Lock()  # 线程锁确保数据库操作安全
        self.failed_orders = []  # 用于记录最终仍失败的订单信息
        self.successful_orders = []  # 新增：用于记录成功的订单信息
        self.processed_chunks = 0  # 用于跟踪已处理的块数
        self.max_retries = 1  # 最大重试次数
        self.retry_interval = 1  # 重试间隔（秒）

    def set_database_session(self, session_maker):
        """设置数据库会话"""
        self.session = session_maker

    def read_excel_chunked(self, chunksize=500):
        """
        生成器函数，手动分块读取大型Excel文件
        """
        # 一次性读取整个文件
        df_full = pd.read_excel(self.filename)
        total_rows = df_full.shape[0]
        print(f"文件总行数: {total_rows}")

        # 计算总块数
        num_chunks = (total_rows // chunksize) + (1 if total_rows % chunksize != 0 else 0)
        print(f"总块数: {num_chunks}")

        # 逐块生成
        for i in range(num_chunks):
            start_index = i * chunksize
            end_index = min((i + 1) * chunksize, total_rows)
            yield df_full.iloc[start_index:end_index]

    def process_chunk_to_orders(self, chunk):
        """将DataFrame块转换为订单对象列表"""
        orders_list = []
        for index, row in chunk.iterrows():
            order_data = {}
            for column in chunk.columns:
                value = row[column]
                if pd.isna(value):
                    order_data[column] = None
                else:
                    order_data[column] = value

            # 这里需要根据你的Excel列名和Order模型字段进行匹配
            order = Order(
                AccountName= "Tinma",
                OrderType="AllOrder",
                OrderId=order_data.get('订单编号'),
                OrderName=order_data.get('订单名称'),
                ProductId=order_data.get('商品编号'),
                PackageName=order_data.get('套餐名称'),
                BrandName=order_data.get('品牌名称'),
                Supplier=order_data.get('供应商'),
                OrderTime=pd.to_datetime(order_data['下单时间']) if order_data.get('下单时间') else None,  # 转换日期字符串
                OrderDepartment=order_data.get('下单部门'),
                OrderUser=order_data.get('下单人'),
                CustomerName=order_data.get('客户名称'),
                DistributorId=order_data.get('分销商编号'),
                SalesClerk=order_data.get('销售专员'),
                Salesman=order_data.get('业务员'),
                SalesmanPhone=order_data.get('业务员电话'),
                GroupDeparture=pd.to_datetime(order_data['出团日期']) if order_data.get('出团日期') else None,
                GroupReturn=pd.to_datetime(order_data['回团日期']) if order_data.get('回团日期') else None,
                TotalPeople=int(order_data['总人数']) if pd.notna(order_data.get('总人数')) else None,
                Adults=int(order_data['成人']) if pd.notna(order_data.get('成人')) else None,
                Children=int(order_data['儿童']) if pd.notna(order_data.get('儿童')) else None,
                Escorts=int(order_data['全陪']) if pd.notna(order_data.get('全陪')) else None,
                AmountPayable=float(order_data['应付']) if pd.notna(order_data.get('应付')) else None,
                AmountPaid=float(order_data['已付']) if pd.notna(order_data.get('已付')) else None,
                AmountUnpaid=float(order_data['未付']) if pd.notna(order_data.get('未付')) else None,
                Rebate=float(order_data['返点']) if pd.notna(order_data.get('返点')) else None,
                ProportionalRebate=float(order_data['按比例返点']) if pd.notna(
                    order_data.get('按比例返点')) else None,
                PerCapitaRebate=float(order_data['按人头返点']) if pd.notna(order_data.get('按人头返点')) else None,
                AmountReceivable=float(order_data['应收']) if pd.notna(order_data.get('应收')) else None,
                AmountReceived=float(order_data['已收']) if pd.notna(order_data.get('已收')) else None,
                AmountUnreceived=float(order_data['未收']) if pd.notna(order_data.get('未收')) else None,
                LossDamage=str(order_data['有损']) if pd.notna(order_data.get('有损')) else False,
                CancelReason=order_data.get('撤单理由'),
                LossCost=float(order_data['损失费用']) if pd.notna(order_data.get('损失费用')) else None,
                SupplierRetainedLoss=float(order_data['供应商自留损失费']) if pd.notna(
                    order_data.get('供应商自留损失费')) else None,
                GrossProfit=float(order_data['毛利']) if pd.notna(order_data.get('毛利')) else None,
                GrossProfitRate=float(order_data['毛利率'].strip('%')) / 100 if (
                        pd.notna(order_data.get('毛利率')) and order_data['毛利率']) else None,  # 处理百分比字符串
                ContactPerson=order_data.get('联系人'),
                ContactPhone=order_data.get('联系人电话'),
                ContractAmount=float(order_data['合同金额']) if pd.notna(order_data.get('合同金额')) else None,
                Tax=float(order_data['税金']) if pd.notna(order_data.get('税金')) else None,
                TourType=order_data.get('跟团类型'),
                ProductRegion=order_data.get('商品地区'),
                ProcessStatus=order_data.get('流程状态'),
                OrderStatus=order_data.get('订单状态'),
                # ... 其他字段 ...
            )
            orders_list.append(order)
        return orders_list

    def retry_single_order_insert(self, session, order, retry_count=0):
        """
        重试单个订单的插入操作[2,3](@ref)

        Args:
            session: 数据库会话
            order: 订单对象
            retry_count: 当前重试次数

        Returns:
            bool: 插入是否成功
        """
        if retry_count >= self.max_retries:
            print(f"订单 {order.OrderId} 已达到最大重试次数 {self.max_retries}，放弃重试")
            return False

        try:
            session.add(order)
            session.flush()
            print(f"订单 {order.OrderId} 重试成功 (尝试次数: {retry_count + 1})")
            return True
        except Exception as e:
            print(f"订单 {order.OrderId} 第 {retry_count + 1} 次重试失败: {e}")
            session.rollback()  # 回滚当前事务
            time.sleep(self.retry_interval)  # 等待一段时间后重试[1](@ref)
            return self.retry_single_order_insert(session, order, retry_count + 1)  # 递归重试

    def write_chunk_to_mssql(self, chunk):
        """将单个数据块写入数据库"""
        if self.session is None:
            raise ValueError("Database session is not set. Please call set_database_session first.")

        session = self.session()
        orders_list = self.process_chunk_to_orders(chunk)
        chunk_failed_orders = []
        chunk_successful_orders = []

        try:
            # 首先尝试批量插入
            with self.lock:
                try:
                    # 尝试一次性添加所有订单
                    for order in orders_list:
                        session.add(order)
                    session.commit()
                    print(f"批量插入成功，共 {len(orders_list)} 条订单数据。")
                    chunk_successful_orders.extend([order.OrderId for order in orders_list])

                except Exception as batch_error:
                    # 如果批量插入失败，回滚并尝试逐个插入
                    print(f"批量插入失败: {batch_error}，开始尝试逐个订单插入...")
                    session.rollback()

                    # 对每个订单尝试插入，失败则重试
                    for order in orders_list:
                        try:
                            session.add(order)
                            session.commit()
                            print(f"订单 {order.OrderId} 直接插入成功")
                            chunk_successful_orders.append(order.OrderId)
                        except Exception as single_error:
                                error_info = f"OrderId: {order.OrderId}, Error: 经过 {self.max_retries} 次重试仍失败"
                                chunk_failed_orders.append(error_info)
                                print(error_info)

        except Exception as e:
            session.rollback()
            print(f"处理块时发生未知错误: {e}")
            # 将整个块的订单标记为失败
            chunk_failed_orders.extend([f"OrderId: {order.OrderId}, Error: {e}" for order in orders_list])
        finally:
            session.close()

        # 更新全局成功和失败列表
        with self.lock:
            self.failed_orders.extend(chunk_failed_orders)
            self.successful_orders.extend(chunk_successful_orders)

        self.processed_chunks += 1
        print(
            f"已处理块数: {self.processed_chunks}, 本块成功: {len(chunk_successful_orders)}, 本块失败: {len(chunk_failed_orders)}")

        return len(chunk_successful_orders), len(chunk_failed_orders)

    def process_single_file_threaded(self, max_workers=4, chunksize=500):
        """使用线程池处理单个文件"""
        print(f"开始使用多线程(线程数: {max_workers})处理文件: {self.filename}")
        print(f"重试配置: 最大重试次数={self.max_retries}, 重试间隔={self.retry_interval}秒")

        self.failed_orders = []
        self.successful_orders = []
        self.processed_chunks = 0
        total_success = 0
        total_failed = 0

        # 使用ThreadPoolExecutor创建线程池
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有块处理任务到线程池
            future_to_chunk = {
                executor.submit(self.write_chunk_to_mssql, chunk): chunk
                for chunk in self.read_excel_chunked(chunksize)
            }

            # 等待所有任务完成并获取结果
            for future in as_completed(future_to_chunk):
                chunk = future_to_chunk[future]
                try:
                    success_count, failed_count = future.result()
                    total_success += success_count
                    total_failed += failed_count
                except Exception as e:
                    print(f"处理块时发生异常: {e}")
                    total_failed += len(chunk)

        # 打印处理结果摘要
        print(f"\n文件处理完成。")
        print(f"总成功行数: {total_success}")
        print(f"总失败行数: {total_failed}")
        print(
            f"成功率: {(total_success / (total_success + total_failed) * 100 if (total_success + total_failed) > 0 else 0):.2f}%")

        if self.failed_orders:
            print(f"\n插入失败的订单信息（共 {len(self.failed_orders)} 条）:")
            for failed_order in self.failed_orders[:10]:  # 最多显示前10条失败信息
                print(failed_order)
            if len(self.failed_orders) > 10:
                print(f"... 以及另外 {len(self.failed_orders) - 10} 条失败记录")


            try:
                with open("failed_orders.log", "w", encoding="utf-8") as f:
                    for order in self.failed_orders:
                        f.write(order + "\n")
                print("失败订单已保存到 failed_orders.log 文件")
            except Exception as e:
                print(f"保存失败订单到文件时出错: {e}")
        else:
            print("\n所有订单均已成功插入！")
    def run(self,writer):
        max_workers = min(32, (os.cpu_count() or 1) * 3)
        SessionMaker_2 = writer.Session

        # 2. 初始化单文件读取器
        data_reader = ReadExcelSingleFile()
        data_reader.set_database_session(SessionMaker_2)

        # 3. 使用多线程处理单个文件
        data_reader.process_single_file_threaded(max_workers=12, chunksize=100)  # 可根据需要调整线程数和块大小


if __name__ == '__main__':
    # 1. 初始化数据库写入器并获取SessionMaker
    writer = MSSQLDatabaseWriter(
        server="gtt-azure-group.database.windows.net",
        database="CruiseCrawlerDB",
        username="majestic",
        password="1AF@8A79f67^0110F7e532C*-21857F8"
    )
