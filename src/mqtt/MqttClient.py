from src.log.Log import Log
from src.config import Config
from src.mqtt.ProcessProtobuf import ProcessProtobuf

from paho.mqtt.client import Client
from threading import Thread


class MqttClient(Thread, ProcessProtobuf):

    __log = None
    __client = None
    
    def __init__(self):
        """ - Recebe os pacotes MQTT enviados do Broker.
            - Utiliza a biblioteca paho.mqtt
            - Armazena as mensagens recebidas em uma fila para que seja processada. 
        Arguments:
            queue {[MqttQueue]} -- [Armazena e processa os pacotes recebidos do MQTT]
        """
        ProcessProtobuf.__init__(self)
        Thread.__init__(self)
        self.__log = Log("MqttClient").start()
        self.__log.info('------------> Initializing MQTT client <------------')

        self.__client = Client()

        self.__client.on_connect = self.on_connect
        self.__client.on_disconnect = self.on_disconnect
        self.__client.on_message = self.on_message

    def run(self):
        """ Fica aguardando pacotes da inscrição
        """

        try:
            self.__client.connect(Config.mqttServer)
            self.__client.loop_forever()
        except Exception as e:
            self.__log.error("%s", e)

    def on_connect(self, client, userdata, flags, rc):
        """ Faz a conexão com o broker e em seguida efetua a inscrição.
        
        Arguments:
            client {[type]} -- [@see paho documents]
            userdata {[type]} -- [@see paho documents]
            flags {[type]} -- [@see paho documents]
            rc {[type]} -- [@see paho documents]
        """

        self.__log.info('MQTT broker %s', "connected." if rc == 0 else "not connected.")

        client.subscribe("Device/Router/+/Gateway/+/#") # ! Da UFAM
        #client.subscribe("D/Rt/+/Gw/+/#")              # ! Do martha

    def on_disconnect(self, client, userdata, rc):
        self.__log.info('MQTT broker disconnected.')

    def on_message(self, client, userdata, message):        
        """Recebe a mensagem MQTT e envia para a fila para que seja decodificada.
        
        Arguments:
            client {[type]} -- [@see paho documents]
            userdata {[type]} -- [@see paho documents]
            message {[type]} -- [Mensagem enviada pelo broker]
        """
        self.addQueue(message)