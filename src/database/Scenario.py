import psycopg2
import pandas as pd

from src.log.Log import Log
from src.config import Config


class Scenario:

    log = None
    database = None
    roomsTable = None
    gatewaysTable = None
    beaconsTable = None
    gatewaysID = {}

    @staticmethod
    def load():
        Scenario.log = Log("Scenario").start()       
        Scenario.log.info('-----------------> Initializing Scenario <-----------------')
        Scenario.log.info("Attempting to connect to the database...")

        try:
            Scenario.database = psycopg2.connect(host=Config.schoodServer, database=Config.schoodDb, 
                                                user=Config.schoodUser, password=Config.schoodPass, 
                                                port=Config.schoodPort, connect_timeout=15)
            
            Scenario.loadAreas()
            Scenario.loadGateways()
            Scenario.loadBeacons()
            Scenario.log.info("connected successfully.")
        except psycopg2.OperationalError as e:                
            Scenario.log.error("%s", str(e))
            
    @staticmethod
    def loadAreas():
        Scenario.log.info("Loading Areas from database...")

        cursor = Scenario.database.cursor()
        cursor.execute("""  SELECT schood.area.id, schood.area.color, schood.area.description,
                            schood.area.ppgroup, schood.area.room_type, schood.area.width,
                            schood.area.height, schood.area.x, schood.area.y, schood.area.floor,
                            schood.area.doors FROM schood.area WHERE schood.area.school_id = """ + 
                            str(Config.schoodSchoolId))
        
        Scenario.log.info("Areas found: %ld", cursor.rowcount)
        
        Scenario.roomsTable = pd.DataFrame(cursor.fetchall(), 
                                        columns=['id', 'color', 'description', 'ppgroup', 'room_type', 'width',
                                                'height', 'x', 'y', 'floor', 'door'])
                
        Scenario.log.info("Areas loaded successfully.")
    
    @staticmethod
    def loadGateways():
        Scenario.log.info("Loading enabled gateways from database...")

        cursor = Scenario.database.cursor()
        cursor.execute("""
            SELECT
                schood.gateway.id,
                schood.gateway.device_id,
                schood.gateway.area_id,
                schood.gateway.rx,
                schood.gateway.rz
            FROM
                schood.gateway
            WHERE
                schood.gateway.school_id = {} AND schood.gateway.enabled = 1 AND schood.gateway.config = 0
            ORDER BY 
                schood.gateway.id """.format(Config.schoodSchoolId)
            )

        Scenario.log.info("Gateways found: %ld", cursor.rowcount)

        Scenario.gatewaysTable = pd.DataFrame(cursor.fetchall(), columns=['id', 'device_id', 'area_id', 'rx', 'rz'])

        x = Scenario.gatewaysTable.loc[:,["id", "device_id"]].set_index("device_id",drop=True)
        Scenario.gatewaysID = x.to_dict("index")
        Scenario.log.info("Gateways loaded successfully.")

    @staticmethod
    def loadBeacons():
        Scenario.log.info("Loading enabled beacons from database...")

        cursor = Scenario.database.cursor()
        cursor.execute("""
                SELECT 
                    schood.bracelet.id,
                    schood.bracelet.bracelet_descriptor, 
                    schood.bracelet.trackable_id, 
                    schood.bracelet.alias, 
                    schoolar.tbl_user.str_first_name
                FROM 
                    schood.bracelet 
                    LEFT JOIN schood.trackable ON schood.bracelet.trackable_id = schood.trackable.id
                    LEFT JOIN schoolar.tbl_user ON schood.trackable.user_id = schoolar.tbl_user.id
                WHERE 
                    schood.bracelet.school_id='{}' AND schood.bracelet.enabled=1 AND schood.bracelet.alias <> ''
                """.format(Config.schoodSchoolId)
            )

        Scenario.log.info("Beacons found: %ld", cursor.rowcount)

        Scenario.beaconsTable = pd.DataFrame(cursor.fetchall(),
                                                columns=['id', 'bracelet_descriptor', 'trackable_id',
                                                        'alias', 'str_first_name'])

        Scenario.log.info("Beacons loaded successfully.")   
    
    @staticmethod
    def getGateways():
        return Scenario.gatewaysTable.id.values

    @staticmethod
    def gatewayIdToMac(id):
        mac = Scenario.gatewaysTable.device_id[Scenario.gatewaysTable.id == id].values

        if mac.size:
            return mac[0]
        else:
            return -1

    @staticmethod
    def beaconIdToMac(id):
        mac = Scenario.beaconsTable.bracelet_descriptor[Scenario.beaconsTable.alias == id].values

        if mac.size:
            return mac[0]
        else:
            return -1

    @staticmethod
    def gatewayMacToId(mac):
        id = Scenario.gatewaysID[mac]['id']
        
        if id:
            return id
        else:
            return -1
    
    @staticmethod
    def beaconMacToId(mac):
        id = Scenario.beaconsTable.alias[Scenario.beaconsTable.bracelet_descriptor == mac].values

        if id.size:
            return id[0]
        else:
            return -1