from CANstruct import *
import time
import pymysql


class ControlCAN:

    def __init__(self, devtype=3, devindex=0, canindex=0, baudrate=250, acccode=0x00000000, accmask=0xFFFFFFFF,
                 ip='localhost', username='root', password='fanxinyuan', schema='candata', rtable='originaldata',
                 ttable='turedata', buffersize=100):
        time0 = {100: 0x04, 125: 0x03, 250: 0x01, 500: 0x00, 1000: 0x00}
        time1 = {100: 0x1C, 125: 0x1C, 250: 0x1C, 500: 0x1C, 1000: 0x14}
        pData = {100: 0x160023, 125: 0x1C0011, 250: 0x1C0008, 500: 0x060007, 1000: 0x060003}
        self.CANdll = WinDLL("ControlCAN.dll")
        self.devtype = devtype
        self.devindex = devindex
        self.canindex = canindex
        self.baudrate = baudrate
        self.time0 = time0[self.baudrate]
        self.time1 = time1[self.baudrate]
        self.time1 = 0x1c
        self.acccode = acccode
        self.accmask = accmask
        self.initconfig = VCI_INIT_CONFIG(self.acccode, self.accmask, 0, 0, self.time0, self.time1, 0)
        self.pData = DWORD(pData[self.baudrate])
        self.errinfo = VCI_ERR_INFO()
        self.boardinfo = VCI_BOARD_INFO()
        self.receivebuf = (VCI_CAN_OBJ * 50)()
        self.sendbuf = VCI_CAN_OBJ()
        self.ctime = time.localtime()
        self.emptynum = 0

        self.vlist = [0] * 16

        self.buffersize = buffersize
        self.datanum = 0
        self.schema = schema
        self.rtable = rtable
        self.ttable = ttable
        self.db = pymysql.connect(ip, username, password, schema)
        self.cursor = self.db.cursor()
        print('连接数据库成功')

    def createtable(self):
        sql = "DROP TABLE IF EXISTS %s.%s" % (self.schema, self.rtable)
        self.cursor.execute(sql)
        sql = "CREATE TABLE `%s`.`%s` (\
            `INDEX` INT UNSIGNED NOT NULL AUTO_INCREMENT,\
            `RealTime` TIMESTAMP(6) NOT NULL,\
            `ID` INT UNSIGNED NOT NULL DEFAULT 0,\
            `TimeStamp` INT UNSIGNED NOT NULL DEFAULT 0,\
            `DataLen` TINYINT(8) UNSIGNED NOT NULL DEFAULT 0,\
            `Data0` TINYINT(8) UNSIGNED NOT NULL DEFAULT 0,\
            `Data1` TINYINT(8) UNSIGNED NOT NULL DEFAULT 0,\
            `Data2` TINYINT(8) UNSIGNED NOT NULL DEFAULT 0,\
            `Data3` TINYINT(8) UNSIGNED NOT NULL DEFAULT 0,\
            `Data4` TINYINT(8) UNSIGNED NOT NULL DEFAULT 0,\
            `Data5` TINYINT(8) UNSIGNED NOT NULL DEFAULT 0,\
            `Data6` TINYINT(8) UNSIGNED NOT NULL DEFAULT 0,\
            `Data7` TINYINT(8) UNSIGNED NOT NULL DEFAULT 0,\
            PRIMARY KEY (`INDEX`));" % (self.schema, self.rtable)
        self.cursor.execute(sql)
        sql = "DROP TABLE IF EXISTS %s.%s" % (self.schema, self.ttable)
        self.cursor.execute(sql)
        sql = "CREATE TABLE `%s`.`%s` (\
            `INDEX` INT UNSIGNED NOT NULL AUTO_INCREMENT,\
            `RealTime` TIMESTAMP(6) NOT NULL,\
            `V0` FLOAT NOT NULL,\
            `V1` FLOAT NOT NULL,\
            `V2` FLOAT NOT NULL,\
            `V3` FLOAT NOT NULL,\
            `V4` FLOAT NOT NULL,\
            `V5` FLOAT NOT NULL,\
            `V6` FLOAT NOT NULL,\
            `V7` FLOAT NOT NULL,\
            `V8` FLOAT NOT NULL,\
            `V9` FLOAT NOT NULL,\
            `V10` FLOAT NOT NULL,\
            `V11` FLOAT NOT NULL,\
            `V12` FLOAT NOT NULL,\
            `V13` FLOAT NOT NULL,\
            `V14` FLOAT NOT NULL,\
            `V15` FLOAT NOT NULL,\
            PRIMARY KEY (`INDEX`));" % (self.schema, self.ttable)
        self.cursor.execute(sql)
        print('创建表格成功')

    def opendevice(self):
        respond = self.CANdll.VCI_OpenDevice(self.devtype, self.devindex, 0)
        if respond:
            print('打开CAN卡成功')
        else:
            print('打开CAN卡失败')
        return respond

    def initcan(self):
        if self.devtype == 21:
            self.CANdll.VCI_SetReference(self.devtype, self.devindex, self.canindex, 0, byref(self.pData))
        respond = self.CANdll.VCI_InitCAN(self.devtype, self.devindex, self.canindex, byref(self.initconfig))
        if respond:
            print('初始化CAN卡成功')
        else:
            print('初始化CAN卡失败')
        return respond

    def startcan(self):
        respond = self.CANdll.VCI_StartCAN(self.devtype, self.devindex, self.canindex)
        if respond:
            print('启动CAN卡成功')
        else:
            print('启动CAN卡失败')
        return respond

    def resetcan(self):
        respond = self.CANdll.VCI_ResetCAN(self.devtype, self.devindex, self.canindex)
        if respond:
            print('复位CAN卡成功')
        else:
            print('复位CAN卡失败')
        return respond

    def readboardinfo(self):
        respond = self.CANdll.VCI_ReadBoardInfo(self.devtype, self.devindex, byref(self.boardinfo))
        if respond:
            print('获取设备信息成功')
        else:
            print('获取设备信息失败')
        return respond

    def receive(self):
        respond = self.CANdll.VCI_Receive(self.devtype, self.devindex, self.canindex, byref(self.receivebuf), 50, 10)
        if respond == 0xFFFFFFFF:
            print('读取数据失败')
            self.CANdll.VCI_ReadErrInfo(self.devtype, self.devindex, self.canindex, byref(self.errinfo))
        elif respond == 0:
            # if self.devtype == 3 or self.devtype == 4:
            #     self.emptynum = self.emptynum + 1
            #     temp = self.emptynum // 20
            #     sys.stdout.write('\r' + "无新数据" + "." * temp)
            #     sys.stdout.flush()
            pass
        elif respond > 0:
            for i in range(respond):
                sql = "INSERT INTO %s(ID,TimeStamp,DataLen,Data0,Data1,Data2,Data3,Data4,Data5,Data6,Data7)\
                                                  VALUES('%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d')" % \
                      (self.rtable,
                       self.receivebuf[i].ID,
                       self.receivebuf[i].TimeStamp,
                       self.receivebuf[i].DataLen,
                       self.receivebuf[i].Data[0],
                       self.receivebuf[i].Data[1],
                       self.receivebuf[i].Data[2],
                       self.receivebuf[i].Data[3],
                       self.receivebuf[i].Data[4],
                       self.receivebuf[i].Data[5],
                       self.receivebuf[i].Data[6],
                       self.receivebuf[i].Data[7])
                self.cursor.execute(sql)

                ID = self.receivebuf[i].ID >> 16
                if ID == 0x841:
                    self.vlist[0] = (self.receivebuf[i].Data[0] * 256 + self.receivebuf[i].Data[1]) / 10000
                    self.vlist[1] = (self.receivebuf[i].Data[0] * 256 + self.receivebuf[i].Data[1]) / 10000
                    self.vlist[2] = (self.receivebuf[i].Data[0] * 256 + self.receivebuf[i].Data[1]) / 10000
                    self.vlist[3] = (self.receivebuf[i].Data[0] * 256 + self.receivebuf[i].Data[1]) / 10000
                elif ID == 0x845:
                    self.vlist[4] = (self.receivebuf[i].Data[0] * 256 + self.receivebuf[i].Data[1]) / 10000
                    self.vlist[5] = (self.receivebuf[i].Data[0] * 256 + self.receivebuf[i].Data[1]) / 10000
                    self.vlist[6] = (self.receivebuf[i].Data[0] * 256 + self.receivebuf[i].Data[1]) / 10000
                    self.vlist[7] = (self.receivebuf[i].Data[0] * 256 + self.receivebuf[i].Data[1]) / 10000
                elif ID == 0x849:
                    self.vlist[8] = (self.receivebuf[i].Data[0] * 256 + self.receivebuf[i].Data[1]) / 10000
                    self.vlist[9] = (self.receivebuf[i].Data[0] * 256 + self.receivebuf[i].Data[1]) / 10000
                    self.vlist[10] = (self.receivebuf[i].Data[0] * 256 + self.receivebuf[i].Data[1]) / 10000
                    self.vlist[11] = (self.receivebuf[i].Data[0] * 256 + self.receivebuf[i].Data[1]) / 10000
                elif ID == 0x84d:
                    self.vlist[12] = (self.receivebuf[i].Data[0] * 256 + self.receivebuf[i].Data[1]) / 10000
                    self.vlist[13] = (self.receivebuf[i].Data[0] * 256 + self.receivebuf[i].Data[1]) / 10000
                    self.vlist[14] = (self.receivebuf[i].Data[0] * 256 + self.receivebuf[i].Data[1]) / 10000
                    self.vlist[15] = (self.receivebuf[i].Data[0] * 256 + self.receivebuf[i].Data[1]) / 10000
                    # print(self.vlist)

                    sql = "INSERT INTO %s(V0,V1,V2,V3,V4,V5,V6,V7,V8,V9,V10,V11,V12,V13,V14,V15)\
                        VALUES('%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f')" % \
                          (self.ttable,
                           self.vlist[0],
                           self.vlist[1],
                           self.vlist[2],
                           self.vlist[3],
                           self.vlist[4],
                           self.vlist[5],
                           self.vlist[6],
                           self.vlist[7],
                           self.vlist[8],
                           self.vlist[9],
                           self.vlist[10],
                           self.vlist[11],
                           self.vlist[12],
                           self.vlist[13],
                           self.vlist[14],
                           self.vlist[15],)
                    self.cursor.execute(sql)

            self.datanum = self.datanum + respond
            if self.datanum > self.buffersize:
                self.db.commit()
                self.datanum = 0

            if self.ctime != time.localtime():
                self.ctime = time.localtime()
                print(time.strftime("%Y-%m-%d %H:%M:%S", self.ctime), end=' ')
                print(self.vlist)

            # f=open('pytxt.txt','a')
            # word = "%s %d\n"%(time.strftime("%Y-%m-%d %H:%M:%S", self.ctime),self.receivebuf[0].TimeStamp)
            # f.write(word)
            # f.close()

        return respond

    def transmit(self):
        respond = self.CANdll.VCI_Transmit(self.devtype, self.devindex, self.canindex, byref(self.sendbuf), 1)
        if respond == 1:
            print('发送CAN帧成功')
        else:
            print('发送CAN帧失败')
        return respond

    def readerrinfo(self):
        respond = self.CANdll.VCI_ReadErrInfo(self.devtype, self.devindex, self.canindex, byref(self.errinfo))
        if respond:
            print('读取错误成功')
        else:
            print('读取错误失败')
        return respond

    def setreference(self):
        respond = self.CANdll.VCI_SetReference(self.devtype, self.devindex, self.canindex, 0, byref(self.pData))
        if respond:
            print('设定E-U波特率成功')
        else:
            print('设定E-U波特率失败')
        return respond

    def getreceivenum(self):
        respond = self.CANdll.VCI_GetReceiveNum(self.devtype, self.devindex, self.canindex)
        return respond

    def __del__(self):
        self.db.close()
        respond = self.CANdll.VCI_CloseDevice(self.devtype, self.devindex)
        if respond:
            print('关闭CAN卡成功')
        else:
            print('关闭CAN卡失败')
        return respond
