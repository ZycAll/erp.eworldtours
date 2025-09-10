import pandas as pd
import itertools

from MSSQL.Create_table import MSSQLDatabaseWriter
from Models.order_model import Order
class ReadExcel:
    def __init__(self):
        self.data = None
        self.filename1 = "excel_xlsx/AllOrder-Eworld.xlsx" #所有订单
        self.filename2 = "excel_xlsx/SupplyOrder-Eworld.xlsx"
        self.filename3 = "excel_xlsx/DistributionOrder-Eworld.xlsx"
        self.filename4 = "excel_xlsx/AllOrder-Tinma.xlsx"
        self.filename5 = "excel_xlsx/SupplyOrder-Tinma2.xlsx"
        self.filename6 = "excel_xlsx/DistributionOrder-Tinma3.xlsx"
        self.filename_li = [self.filename1, self.filename2, self.filename3,self.filename4,self.filename5,self.filename6]
        self.session = None
    def read_excel(self,filename):
        self.data = pd.read_excel(filename)
        return self.data

    def set_database_session(self, session_maker):
        """设置数据库会话"""
        self.session = session_maker
    def write_to_mssql(self, df,order_type):
        """
        将DataFrame数据批量写入SQL Server数据库
        :param df: 包含订单数据的DataFrame
        """
        if self.session is None:
            raise ValueError("Database session is not set. Please call set_database_session first.")
        session = self.session()  # 创建一个新的会话实例
        try:
            orders_list = []
            # 遍历DataFrame的每一行
            for index, row in df.iterrows():
                # 将NaN替换为None，并处理其他可能的数据类型转换
                order_data = {}
                for column in df.columns:
                    value = row[column]
                    # 检查是否为NaN（浮点数NaN）或NaT（时间戳NaN）
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

            # 批量添加所有订单对象到session
            session.bulk_save_objects(orders_list)
            # 提交事务
            session.commit()
            print(f"成功插入 {len(orders_list)} 条订单数据。")

        except Exception as e:
            # 发生错误时回滚事务
            session.rollback()
            print(f"插入数据时发生错误: {e}")
            raise
        finally:
            # 关闭会话
            session.close()

if __name__ == '__main__':
    # 1. 初始化数据库写入器并获取SessionMaker
    writer = MSSQLDatabaseWriter(
        server="gtt-azure-group.database.windows.net",
        database="CruiseCrawlerDB",
        username="majestic",
        password="1AF@8A79f67^0110F7e532C*-21857F8"
    )
    SessionMaker = writer.Session

    data_reader = ReadExcel()

    data_reader.set_database_session(SessionMaker)
    num = 4
    for filename in data_reader.filename_li[3:]:
        order_type = None
        if num == 1:
            order_type = "AllOrder-Eworld"
        if num == 2:
            order_type = "SupplyOrder-Eworld"
        if num == 3:
            order_type = "DistributionOrder-Eworld"
        if num == 4:
            order_type = "AllOrder-Tinma"
        if num == 5:
            order_type = "SupplyOrder-Tinma"
        if num == 6:
            order_type = "DistributionOrder-Tinma"
        df = data_reader.read_excel(filename)
        df_subset = df
        data_reader.write_to_mssql(df_subset,order_type=order_type)
        num +=1
        order_num = len(df_subset)
        print(f"{order_type}的{order_num}个订单已经写入")
