import logging

"""
 - Gera logs individuais para cada instância gerada. 
 - Os logs são salvos dentro de uma pasta logo no mesmo caminho do main.py
 - O nomes dos logs são os nomes das classes que o instanciaram (passado manaualemten, automatizar ?)
"""


class Log:

    handler = None
    logger = None

    def __init__(self, name):
        """Configura o logger para salvar o arquivo com o nome
            passado por parâmetro.

        Arguments:
            name {[String]} -- [O nome do arquivo]
        """

        self.handler = logging.FileHandler("logs/" + name + ".log")
        self.handler.setFormatter(logging.Formatter('%(asctime)s (%(levelname)s) \t---> %(message)s'))
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(self.handler)

    def start(self):
        """Retorna o logger configurado.

        Returns:
            [Logger] -- [Instância do Logger]
        """

        return self.logger
