from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, DateTime, Integer, Numeric, Boolean, Text, func, ForeignKey, NVARCHAR
import uuid

Base = declarative_base()

class Order(Base):
    __tablename__ = 'Eworldtours_AllOrders'  # 自定义表名，避免使用SQL关键字

    Id = Column(NVARCHAR(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    OrderType = Column(NVARCHAR(36))

    OrderId = Column(NVARCHAR(36), unique=True) #订单编号
    OrderName = Column(NVARCHAR(255))  # 订单名称
    ProductId = Column(NVARCHAR(50))   # 商品编号
    PackageName = Column(NVARCHAR(255)) # 套餐名称
    BrandName = Column(NVARCHAR(255))  # 品牌名称
    Supplier = Column(NVARCHAR(255))   # 供应商
    OrderTime = Column(DateTime) # 下单时间
    OrderDepartment = Column(NVARCHAR(255)) # 下单部门
    OrderUser = Column(NVARCHAR(255))   # 下单人
    CustomerName = Column(NVARCHAR(255)) # 客户名称

    DistributorId = Column(NVARCHAR(255)) # 分销商编号
    SalesClerk = Column(NVARCHAR(255))  # 销售专员
    Salesman = Column(NVARCHAR(255))    # 业务员
    SalesmanPhone = Column(NVARCHAR(50)) # 业务员电话
    GroupDeparture = Column(DateTime) # 出团日期
    GroupReturn = Column(DateTime)    # 回团日期
    TotalPeople = Column(Integer)     # 总人数
    Adults = Column(Integer)          # 成人
    Children = Column(Integer)        # 儿童
    Escorts = Column(Integer)         # 全陪

    AmountPayable = Column(Numeric(16, 4)) # 应付
    AmountPaid = Column(Numeric(16, 4))   # 已付
    AmountUnpaid = Column(Numeric(16, 4)) # 未付
    Rebate = Column(Numeric(16, 4))   # 返点
    ProportionalRebate = Column(Numeric(16, 4)) # 按比例返点
    PerCapitaRebate = Column(Numeric(16, 4)) # 按人头返点
    AmountReceivable = Column(Numeric(16, 4)) # 应收
    AmountReceived = Column(Numeric(16, 4))  # 已收
    AmountUnreceived = Column(Numeric(16, 4)) # 未收
    LossDamage = Column(NVARCHAR(255)) # 有损

    CancelReason = Column(NVARCHAR(500))       # 撤单理由
    LossCost = Column(Numeric(16, 4)) # 损失费用
    SupplierRetainedLoss = Column(Numeric(16, 4)) # 供应商自留损失费
    GrossProfit = Column(Numeric(16, 4)) # 毛利
    GrossProfitRate = Column(Numeric(10, 6)) # 毛利率
    ContactPerson = Column(NVARCHAR(255)) # 联系人
    ContactPhone = Column(NVARCHAR(50))  # 联系人电话
    ContractAmount = Column(Numeric(16, 4)) # 合同金额
    Tax = Column(Numeric(16, 4))      # 税金
    TourType = Column(NVARCHAR(255))    # 跟团类型 (考虑使用 Enum)

    ProductRegion = Column(NVARCHAR(255)) # 商品地区
    ProcessStatus = Column(NVARCHAR(255)) # 流程状态 (考虑使用 Enum)
    OrderStatus = Column(NVARCHAR(255)) # 订单状态 (考虑使用 Enum)



    BranchRebates = Column(Numeric(16, 4))   #分公司返点
    Promotions = Column(Numeric(16, 4))  #促销

    CreatedAt = Column(DateTime, server_default=func.now())  # 记录创建时间