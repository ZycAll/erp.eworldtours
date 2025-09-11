from MSSQL.Create_table import MSSQLDatabaseWriter


class GetOrderId(MSSQLDatabaseWriter):
    def __init__(self, server, database, username, password):
        super().__init__(server, database, username, password)
    def GetOrderId_byOrderType(self):

