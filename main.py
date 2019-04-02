from src.mqtt.MqttClient import MqttClient
from src.database.Scenario import Scenario
from src.config import Config

Scenario.load()             # * Carrega os beacons e gateways do banco de dados       

mqtt = MqttClient()         # * Instância que vai se conectar e receber os dados binários  
mqtt.daemon = True
mqtt.start()  


input("Press Enter to exit program ...\n") # ! Segura o programa (evita que as threads acima sejam encerradas)