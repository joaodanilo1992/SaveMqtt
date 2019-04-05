
import os
import time


import pandas as pd
from datetime import datetime
from queue import Queue
from threading import Thread


from src.log.Log import Log
from src.database.Scenario import Scenario
from src.protobuf import xrbo_pb2



# from read_protobuf import read_protobuf


class ProcessProtobuf:

    __queue = None
    __columns = None
    __data = {}

    def __init__(self):        
        self.__log = Log("ProcessProtobuf").start()
        self.__log.info('------------> Initializing Process Protobuf <------------')

        self.__queue = Queue()

        self.__columns = ["WAP" + str(s) for s in Scenario.getGateways().tolist()]
        self.__columns.append("BEACONID") 
        self.__columns.append("TIMESTAMP")

        thread = Thread(target=self.processQueue)
        thread.daemon = True
        thread.start()
    
    def addQueue(self, pkt):
        self.__queue.put(pkt)
    
    def processQueue(self):
        while True:        
            while not self.__queue.empty():    
                msg = self.__queue.get()
                self.decodeProtobuf(msg) 
            
            if self.__data:           
                self.generateFile(self.__data)
                self.__data.clear()
            time.sleep(1)
    
    def decodeProtobuf(self, rawPkt):
        
        topicSplit = rawPkt.topic.split("/")
        gateway = Scenario.gatewayMacToId(topicSplit[4].upper())

        pkt = xrbo_pb2.DataForward()
        pkt.ParseFromString(rawPkt.payload)

        for field in pkt.ListFields():
            if field[0].type == field[0].TYPE_MESSAGE:
                self.processProtobuf(pkt, gateway) 
    
    def processProtobuf(self, msg, gateway):
        """ Gera um dicionário com os dados de interesse contidos no protobuf.
        Arguments:
            msg {[xrbo_pb2.DataForward]} -- [Mensagem dos beacons MQTT]
            gateway {[int]} -- [O gateway que escutou a mensagem]
        """
        # data = {}
        for i in range(msg.bt_count):
            beacon = msg.bt[i].mac.upper()
            rssi = int(msg.bt[i].rssi)

            # data[beacon] = {gateway: rssi}
            if beacon not in self.__data:
                self.__data[beacon] = { gateway: rssi }
            else:
                if gateway not in self.__data[beacon] or rssi > self.__data[beacon][gateway]:
                    self.__data[beacon][gateway] = rssi
    
    def generateFile(self, data):
        """ Gera o arquivo no formato em que deve ser salvo (com todos os gateways da base)
        
        Arguments:
            data {[dict]} -- [Mensagem do gateway]
        """

        
        beacons = pd.Series(list(data.keys()), dtype=str)
        
        dataBlock = pd.DataFrame(columns=self.__columns)
        dataBlock["BEACONID"] = pd.concat([dataBlock["BEACONID"], beacons], sort=True, ignore_index=True)

        newData = pd.DataFrame.from_dict(data, dtype='Int64')
        
        dataBlock[["WAP" + str(gateway) for gateway in newData.T.columns.values]] = newData.T.values
        dataBlock["TIMESTAMP"] = datetime.now().timestamp()
        
        dataBlock.fillna(100, inplace=True)


        self.saveFile(dataBlock)
    
    def saveFile(self, dataBlock):
        
        now = datetime.now()
        path = "logs/" + now.strftime("%Y-%m-%d") + "/"
        nameFile = "Base.csv"
        if(not os.path.exists(path)):
            os.mkdir(path)
            dataBlock.to_csv(path + nameFile, index=False, mode='a', header=True)
        else:        
            dataBlock.to_csv(path + nameFile, index=False, mode='a', header=False)
        
        # for row in dataBlock.iterrows():                                          #
        #     with open(path + str(row[1][-1]) + ".csv", 'a') as f:                 # Salva os beacons de forma individual
        #         row[1].to_frame().T.to_csv(f, index=False, header=f.tell()==0)    #


    """ --------------------------------------------------
                Métodos para testes
        --------------------------------------------------
    """


    # def decodeProtobuf(self, rawPkt):
    #     topicSplit = rawPkt.topic.split("/")        
    #     gateway = Scenario.gatewayMacToId(topicSplit[4].upper())

    #     try:
    #         decodedPayload = read_protobuf([rawPkt.payload], xrbo_pb2.DataForward())            
            
    #         beaconsData = decodedPayload.bt.apply(pd.Series)             
    #         beaconsData = beaconsData.assign(gateway=pd.Series(["WAP" + str(gateway)]*beaconsData.shape[0]))
            
    #         if 'data' in beaconsData:
    #             beaconsData.drop(columns=['data'], inplace=True)
            
    #         if 'time' in beaconsData:
    #             beaconsData.drop(columns=['time'], inplace=True)            
            
    #         self.__queue.put(beaconsData) # ! Thread safe

    #     except Exception as e:  # ! Se bt_count = 0
    #         x = xrbo_pb2.DataForward()
    #         x.ParseFromString(rawPkt.payload)           
    #         self.__log.error("Gateway: " + str(gateway) + " " + str(x).replace('\n', " "))            
    #         self.__log.debug("Gateway has no beacons")
    #         self.__log.error(e)
    
    # def assemleBlock(self):
    #     while True:
    #         dataBlock = None
    #         dataframe = None
            
    #         if not self.__queue.empty():

    #             for elem in list(self.__queue.queue):              
    #                 dataframe = pd.concat([dataframe, elem], ignore_index=True, sort=False)
    #             self.__queue.queue.clear()

    #             columns = ["WAP" + str(s) for s in Scenario.getGateways().tolist()]
    #             columns.append("BEACONID")  

    #             dataBlock = pd.DataFrame(columns=columns)
    #             beacons = pd.Series(dataframe.mac.unique())

    #             dataBlock["BEACONID"] = pd.concat([dataBlock["BEACONID"], beacons], sort=True, ignore_index=True)
                
    #             for gateway in dataframe.gateway.unique():

    #                 sameGateway = dataframe[dataframe.gateway == gateway]
    #                 sameGateway = sameGateway.sort_values("rssi", ascending=False)
    #                 sameGateway = sameGateway.loc[sameGateway.duplicated("mac") == False, ["mac", "rssi"]]
                    
    #                 for beacon in sameGateway.mac:
    #                     rssi = sameGateway.loc[sameGateway.mac == beacon, "rssi"].values                    
    #                     dataBlock.loc[dataBlock.BEACONID==beacon,gateway] = rssi
                        
    #             dataBlock.fillna(100, inplace=True)
                
    #             self.saveFile(dataBlock)
    #         time.sleep(1)
    
    