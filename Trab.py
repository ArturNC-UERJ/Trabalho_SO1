from threading import Thread, Lock

from queue import Queue

from collections  import defaultdict

import time

from random import choice, randint





# Classe para o publicador

class Publicador(Thread):



    # Inicializacao, associa um id e um broker

    def __init__(self, id, broker):

        super().__init__()

        self.id = id

        self.broker = broker



    # Executa a logica de escolher o topico aleatorio e chama a funcao do broker com uma mensagem aleatoria (sobrescreve a função Run do import threading)

    def run(self):

        while True:

            topico = choice(["esportes", "notícias", "tecnologia"])

            mensagem = randint(1, 100)

            print(f"Mensagem de {self.id} para {topico}: {mensagem}")

            self.broker.publicar(topico, mensagem)

            time.sleep(2)



# Assinante (Cliente)

class Assinante(Thread):



    # Inicializacao, associa um id, um broker e um topico para o assinante

    def __init__(self, id, broker, topico):

        super().__init__()

        self.id = id

        self.broker = broker

        self.topico = topico



    # Executa o registro do assinante no topico passado

    def run(self):

        self.broker.registrar_assinante(self.topico, self)

        while True:

            time.sleep(2)



    def receber_mensagem(self, mensagem):

        print(f"{self.id} recebeu mensagem: Notícia de {self.topico} {mensagem}")



# Broker central

class Broker(Thread):

    def __init__(self):



        self.filas = defaultdict(Queue)   # Fila de mensagens para cada tópico

        self.assinantes = defaultdict(list)   # Dicionario de assinantes para cada tópico

        self.lock = Lock()   # Para controlar o acesso simultaneo das Threads



    def publicar(self, topico, mensagem):



        # Evita concorrencia e valida se existe algum assinante para o topico

        with self.lock:

            if topico in self.assinantes and len(self.assinantes[topico]) > 0:

                print(f"Broker: Publicando mensagem para tópico '{topico}': {mensagem}")

                self.filas[topico].put(mensagem)

            else:

                print(f"Broker: Nenhum assinante para o tópico '{topico}'. Mensagem descartada.")



    def registrar_assinante(self, topico, assinante):



        # Evita concorrencia e registra um assinante ao dicionario de topicos

        with self.lock:

            self.assinantes[topico].append(assinante)

            print(f"{assinante.id} registrado no tópico '{topico}'")



    def entregar_mensagens(self):



        # Executa continuamente e verifica concorrencia com o .lock, 

        while True:

            with self.lock:

                for topico, fila in self.filas.items():

                    # Verifica existencia de mensagens

                    if not fila.empty():

                        # Retorna um item da fila removendo ele dela

                        mensagem = fila.get()

                        for assinante in self.assinantes[topico]:

                            assinante.receber_mensagem(mensagem)

                            print()

            time.sleep(2)



# Programa Principal

if __name__ == "__main__":



    # Inicializando o Broker

    broker = Broker()



    # Criando publicadores e inicializando as threads

    publicador1 = Publicador("Publicador 1", broker).start()

    publicador2 = Publicador("Publicador 2", broker).start()

    publicador3 = Publicador("Publicador 3", broker).start()



    # Criando assinantes e inicializando as threads

    assinante1 = Assinante("Assinante 1", broker, "esportes").start()

    assinante2 = Assinante("Assinante 2", broker, "notícias").start()

    assinante2 = Assinante("Assinante 3", broker, "notícias").start()

    assinante3 = Assinante("Assinante 4", broker, "tecnologia").start()



    # Broker entregando mensagens continuamente

    broker.entregar_mensagens()
