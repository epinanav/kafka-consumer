package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Obtener la dirección del broker de Kafka desde la variable de entorno
	broker := "b-1.mskdearneuronkafka.55zwdr.c3.kafka.eu-west-1.amazonaws.com"
	if broker == "" {
		fmt.Println("La variable de entorno KAFKA_BROKER no está configurada.")
		return
	}

	// Configurar el cliente de Kafka
	conf := &kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          "my-consumer-group", // Aquí debes especificar un ID único para tu grupo de consumidores
	}
	

	// Crear un nuevo consumidor Kafka
	c, err := kafka.NewConsumer(conf)
	if err != nil {
		fmt.Printf("Error al crear el consumidor Kafka: %v\n", err)
		return
	}
	defer c.Close()

	// Suscribirse a un tema
	topics := []string{"WDN"}
	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		fmt.Printf("Error al suscribirse a los temas: %v\n", err)
		return
	}

	// Configurar un canal para manejar las señales de terminación
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Iniciar el bucle de consumo de mensajes
	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Terminación debido a la señal: %v\n", sig)
			run = false
		default:
			// Esperar por un mensaje
			msg, err := c.ReadMessage(-1)
			if err == nil {
				fmt.Printf("Mensaje recibido: %s\n", string(msg.Value))
			} else {
				fmt.Printf("Error al leer el mensaje: %v\n", err)
			}
		}
	}
}
