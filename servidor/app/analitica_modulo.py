from datetime import datetime
import pandas as pd
import numpy as np
import os
from sklearn.linear_model import LinearRegression
import time
import pika
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish

class analitica():
    ventana = 10
    pronostico = 3
    file_name = "data_base.csv"
    servidor = "52.22.75.246"

    def on_connect(client, userdata, flags, rc):
        print("Connected with result code "+str(rc))
        client.subscribe("$SYS/#")

    def on_message(client, userdata, msg):
        print(msg.topic + " " + str(msg.payload))

    client = mqtt.Client('Publicador_alerta')
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(servidor, 1883, 60)
    client.loop_start()

    def __init__(self) -> None:
        self.load_data()

    def load_data(self):

        if not os.path.isfile(self.file_name):
            self.df = pd.DataFrame(columns=["fecha", "sensor", "valor"])
        else:
            self.df = pd.read_csv (self.file_name)

    def update_data(self, msj):
        msj_vetor = msj.split(",")
        now = datetime.now()
        date_time = now.strftime('%d.%m.%Y %H:%M:%S')
        new_data = {"fecha": date_time, "sensor": msj_vetor[0], "valor": float(msj_vetor[1])}
        self.df = self.df.append(new_data, ignore_index=True)
        new_data = {"fecha": date_time, "sensor": msj_vetor[2], "valor": float(msj_vetor[3])}
        self.df = self.df.append(new_data, ignore_index=True)
        new_data = {"fecha": date_time, "sensor": msj_vetor[4], "valor": float(msj_vetor[5])}
        self.df = self.df.append(new_data, ignore_index=True)
        new_data = {"fecha": date_time, "sensor": msj_vetor[6], "valor": float(msj_vetor[7])}
        self.df = self.df.append(new_data, ignore_index=True)
        new_data = {"fecha": date_time, "sensor": msj_vetor[8], "valor": float(msj_vetor[9])}
        self.df = self.df.append(new_data, ignore_index=True)
        new_data = {"fecha": date_time, "sensor": msj_vetor[10], "valor": float(msj_vetor[11])}
        self.df = self.df.append(new_data, ignore_index=True)

        self.publicar("EjeX",msj_vetor[1])
        self.publicar("EjeY",msj_vetor[3])
        self.publicar("EjeZ",msj_vetor[5])
        self.publicar("temp_data",msj_vetor[7])
        self.publicar("pres_data",msj_vetor[9])
        self.publicar("humd_data",msj_vetor[11])
     
        self.analitica_descriptiva()
        self.analitica_predictiva()
        self.guardar()

    def print_data(self):
        print(self.df)

    def analitica_descriptiva(self):
        self.operaciones("EjeX")
        self.operaciones("EjeY")
        self.operaciones("EjeZ")
        self.operaciones("temp_data")
        self.operaciones("pres_data")
        self.operaciones("humd_data")


    def operaciones(self, sensor):
        df_filtrado = self.df[self.df["sensor"] == sensor]
        df_filtrado = df_filtrado["valor"]
        df_filtrado = df_filtrado.tail(self.ventana)
        self.publicar("max-{}".format(sensor), str(df_filtrado.max(skipna = True)))
        self.publicar("min-{}".format(sensor), str(df_filtrado.min(skipna = True)))
        self.publicar("mean-{}".format(sensor), str(df_filtrado.mean(skipna = True)))
        self.publicar("median-{}".format(sensor), str(df_filtrado.median(skipna = True)))
        self.publicar("std-{}".format(sensor), str(df_filtrado.std(skipna = True)))

        if (("max-{}".format(sensor)=="max-EjeX".format(sensor)) and str(df_filtrado.max(skipna = True))>"70") or (("max-{}".format(sensor)=="max-EjeY".format(sensor)) and str(df_filtrado.max(skipna = True))>"70") or (("max-{}".format(sensor)=="max-EjeZ".format(sensor)) and str(df_filtrado.max(skipna = True))>"70"):
            
            self.publicar("alert-derrumbe".format(sensor), "Precaucion cambio abrupto")
            publish.single('2/alert-derrumbe', "Precaucion cambio abrupto", hostname='52.22.75.246', client_id='alert')
            
        if (("min-{}".format(sensor)=="min-EjeX".format(sensor)) and str(df_filtrado.min(skipna = True))<"70") or (("min-{}".format(sensor)=="min-EjeY".format(sensor)) and str(df_filtrado.min(skipna = True))<"70") or (("min-{}".format(sensor)=="min-EjeZ".format(sensor)) and str(df_filtrado.min(skipna = True))<"70"):

            self.publicar("alert-derrumbe".format(sensor), "Precaucion cambio abrupto")
            publish.single('2/alert-derrumbe', "Precaucion cambio abrupto", hostname='52.22.75.246', client_id='alert')

        if ("max-{}".format(sensor)=="max-humd_data".format(sensor)) and str(df_filtrado.max(skipna = True))>"70":
            publish.single('2/alertahumd_data', "Humedad elevada", hostname='52.22.75.246', client_id='alert')
            #self.publicar("alerta-humedad".format(sensor), "Humedad Elevada ")

        if ("min-{}".format(sensor)=="min-humd_data".format(sensor)) and str(df_filtrado.min(skipna = True))<"60":
            publish.single('2/alertahumd_data', "Humedad baja", hostname='52.22.75.246', client_id='alert')
            #self.publicar("alerta-humd_data".format(sensor), "Humedad Muy Baja")


    def analitica_predictiva(self):
        self.regresion("temp_data")
        self.regresion("pres_data")
        self.regresion("humd_data")


    def regresion(self, sensor):
        df_filtrado = self.df[self.df["sensor"] == sensor]
        df_filtrado = df_filtrado.tail(self.ventana)
        df_filtrado['fecha'] = pd.to_datetime(df_filtrado.pop('fecha'), format='%d.%m.%Y %H:%M:%S')
        df_filtrado['segundos'] = [time.mktime(t.timetuple()) - 18000 for t in df_filtrado['fecha']]
        tiempo = df_filtrado['segundos'].std(skipna = True)
        if np.isnan(tiempo):
            return
        tiempo = int(round(tiempo))
        ultimo_tiempo = df_filtrado['segundos'].iloc[-1]
        ultimo_tiempo = ultimo_tiempo.astype(int)
        range(ultimo_tiempo + tiempo,(self.pronostico + 1) * tiempo + ultimo_tiempo, tiempo)
        nuevos_tiempos = np.array(range(ultimo_tiempo + tiempo,(self.pronostico + 1) * tiempo + ultimo_tiempo, tiempo))

        X = df_filtrado["segundos"].to_numpy().reshape(-1, 1)
        Y = df_filtrado["valor"].to_numpy().reshape(-1, 1)
        linear_regressor = LinearRegression()
        linear_regressor.fit(X, Y)
        Y_pred = linear_regressor.predict(nuevos_tiempos.reshape(-1, 1))
        for tiempo, prediccion in zip(nuevos_tiempos, Y_pred):
            time_format = datetime.utcfromtimestamp(tiempo)
            date_time = time_format.strftime('%d.%m.%Y %H:%M:%S')
            self.publicar("prediccion-{}".format(sensor), "{},{}".format(date_time,prediccion[0]))
            self.publicar("prediccion_datos-{}".format(sensor), "{:.2f}".format(prediccion[0]))
    @staticmethod
    def publicar(cola, mensaje):
        connexion = pika.BlockingConnection(pika.ConnectionParameters(host='rabbit'))
        canal = connexion.channel()
        # Declarar la cola
        canal.queue_declare(queue=cola, durable=True)
        # Publicar el mensaje
        canal.basic_publish(exchange='', routing_key=cola, body=mensaje)
        # Cerrar conexión
        connexion.close()

    def guardar(self):
        self.df.to_csv(self.file_name, encoding='utf-8')