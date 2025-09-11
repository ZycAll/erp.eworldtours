import pandas as pd
import itertools
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from MSSQL.Create_table import MSSQLDatabaseWriter
from Models.order_model import Order


class ReadExcel:
    def __init__(self):
        self.data = None
        self.filename1 = "../excel_xlsx/AllOrder-Eworld.xlsx"  # 所有订单
        self.filename2 = "../excel_xlsx/SupplyOrder-Eworld.xlsx"
        self.filename3 = "../excel_xlsx/DistributionOrder-Eworld.xlsx"
        #self.filename4 = "../excel_xlsx/AllOrder-Tinma.xlsx"
        self.filename5 = "../excel_xlsx/SupplyOrder-Tinma.xlsx"
        self.filename6 = "../excel_xlsx/DistributionOrder-Tinma.xlsx"
        self.filename_li = [self.filename1, self.filename2, self.filename3,  self.filename5, #self.filename4,
                            self.filename6]
        self.session = None
        self.lock = threading.Lock()  # 添加线程锁确保数据库操作安全
        self.failed_orders = []  # 新增：用于记录失败的订单编号和错误信息

    def read_excel(self, filename, ):
        """分块读取Excel文件"""
        chunk_list = []
        for chunk in pd.read_excel(filename):
            chunk_list.append(chunk)
        return pd.concat(chunk_list, ignore_index=True)

    def read_excel_chunked(self, filename, chunksize=500):
        """生成器函数，手动分块读取大型Excel文件"""
        # 一次性读取整个文件
        df_full = pd.read_excel(filename)
        total_rows = df_full.shape[0]

        # 计算总块数
        num_chunks = (total_rows // chunksize) + (1 if total_rows % chunksize != 0 else 0)

        # 逐块生成
        for i in range(num_chunks):
            start_index = i * chunksize
            end_index = min((i + 1) * chunksize, total_rows)
            yield df_full.iloc[start_index:end_index]

    def set_database_session(self, session_maker):
        """设置数据库会话"""
        self.session = session_maker

    def process_chunk_to_orders(self, chunk, order_type):
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
            order = Order(
                OrderType=order_type,
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

    def write_to_mssql(self, df, order_type):
        """
        将DataFrame数据批量写入SQL Server数据库
        """
        if self.session is None:
            raise ValueError("Database session is not set. Please call set_database_session first.")

        session = self.session()
        orders_list = self.process_chunk_to_orders(df, order_type)
        chunk_failed_orders = []
        try:
            # 使用线程锁确保数据库操作安全[4](@ref)
            with self.lock:
                for order in orders_list:
                    try:
                        # 尝试逐个添加订单对象，但暂不提交
                        session.add(order)
                        # 可以尝试 flush 来立即执行插入操作，但遇到错误会抛出异常
                        session.flush()
                        print(f"{order.OrderId} ok")
                    except Exception as e:
                        # 如果插入失败，回滚当前事务（针对这个订单）
                        session.rollback()
                        # 记录失败的订单编号和错误信息
                        print(f"{order.OrderId} error: {e}")
                        order_info = f"OrderType: {order_type}, OrderId: {order.OrderId}, Error: {str(e)}"
                        chunk_failed_orders.append(order_info)
                        # 继续处理下一个订单
                        continue
                # 如果所有订单都成功，提交事务
                session.commit()
                print(f"成功插入 {len(orders_list) - len(chunk_failed_orders)} 条订单数据。")

        except Exception as e:
            # 处理可能出现的其他异常
            session.rollback()
            print(f"处理块时发生未知错误: {e}")
        finally:
            session.close()

            # 将当前块的失败订单添加到总列表中
        with self.lock:  # 确保多线程下对共享资源 self.failed_orders 的访问安全[4](@ref)
            self.failed_orders.extend(chunk_failed_orders)

    def process_single_file(self, filename, order_type, chunksize=500):
        """处理单个文件（支持分块读取）"""
        print(f"开始处理文件: {filename}")
        try:
            # 分块读取并处理
            for chunk in self.read_excel_chunked(filename, chunksize=chunksize):
                self.write_to_mssql(chunk, order_type)

            order_num = len(pd.read_excel(filename))
            print(f"{order_type}的{order_num}个订单已经尝试处理")
            return True

        except Exception as e:
            print(f"处理文件 {filename} 时发生错误: {e}")
            return False

    def process_all_files_threaded(self, max_workers=3):
        """使用线程池处理所有文件"""
        # 文件与订单类型的映射
        file_type_mapping = {
            self.filename1: "AllOrder-Eworld",
            self.filename2: "SupplyOrder-Eworld",
            self.filename3: "DistributionOrder-Eworld",
            self.filename4: "AllOrder-Tinma",
            self.filename5: "SupplyOrder-Tinma",
            self.filename6: "DistributionOrder-Tinma"
        }
        self.failed_orders = []
        # 使用ThreadPoolExecutor创建线程池[6](@ref)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务到线程池
            future_to_file = {
                executor.submit(self.process_single_file, filename, file_type_mapping[filename]): filename
                for filename in self.filename_li[3:]
            }

            # 等待所有任务完成
            success_count = 0
            for future in as_completed(future_to_file):
                filename = future_to_file[future]
                try:
                    result = future.result()
                    if result:
                        success_count += 1
                        print(f"文件 {filename} 处理成功")
                    else:
                        print(f"文件 {filename} 处理失败")
                except Exception as e:
                    print(f"文件 {filename} 处理时发生异常: {e}")

            print(f"所有文件处理完成。成功: {success_count}/{len(self.filename_li)}")

        # 所有线程结束后，打印失败的订单信息
        if self.failed_orders:
            print(f"\n以下是插入失败的订单信息（共 {len(self.failed_orders)} 条）:")
            for failed_order in self.failed_orders:
                print(failed_order["OrderId"])
        else:
            print("\n所有订单均已成功插入！")
if __name__ == '__main__':
    # 1. 初始化数据库写入器并获取SessionMaker
    writer = MSSQLDatabaseWriter(
        server="gtt-azure-group.database.windows.net",
        database="CruiseCrawlerDB",
        username="gttadmin",
        password="EC90-18AC15y&00FE8086*4B286e3901"
    )
    SessionMaker = writer.Session

    data_reader = ReadExcel()
    data_reader.set_database_session(SessionMaker)

    # 使用多线程处理所有文件
    data_reader.process_all_files_threaded(max_workers=5)  # 可以根据需要调整线程数