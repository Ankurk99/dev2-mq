package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	feeder "github.com/kubearmor/KubeArmor/protobuf"
	"github.com/kubearmor/kubearmor-client/k8s"
	klog "github.com/kubearmor/kubearmor-client/log"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	serverAddress = "localhost:32767"
	queueName     = "logs"
)

type Log struct {
	Timestamp   int64  `protobuf:"varint,1,opt,name=Timestamp" json:"Timestamp"`
	UpdatedTime string `protobuf:"bytes,2,opt,name=UpdatedTime" json:"UpdatedTime"`

	ClusterName string `protobuf:"bytes,3,opt,name=ClusterName" json:"ClusterName"`
	HostName    string `protobuf:"bytes,4,opt,name=HostName" json:"HostName"`
	HostIP      string `protobuf:"bytes,5,opt,name=HostIP" json:"HostIP"`

	Type    string `protobuf:"bytes,6,opt,name=Type" json:"Type"`
	Level   string `protobuf:"bytes,7,opt,name=Level" json:"Level"`
	Message string `protobuf:"bytes,8,opt,name=Message" json:"Message"`
}

func main() {
	// Create a RabbitMQ connection
	connection, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer connection.Close()

	// Create a RabbitMQ channel
	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("Failed to create RabbitMQ channel: %v", err)
	}
	defer channel.Close()

	// Declare the queue
	queue, err := channel.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare RabbitMQ queue: %v", err)
	}

	eventChan := make(chan klog.EventInfo)
	client, err := k8s.ConnectK8sClient()
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		err := klog.StartObserver(client, klog.Options{
			LogFilter: "all",
			MsgPath:   "none",
			EventChan: eventChan,
			GRPC:      serverAddress,
		})

		if err != nil {
			return
		}
	}()

	// Listen for interrupt signal (Ctrl+C) and stop the program gracefully
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		log.Println("Stopping the program...")
		os.Exit(0)
	}()

	for evtin := range eventChan {
		if evtin.Type == "Log" {
			tel := feeder.Log{}
			err := protojson.Unmarshal(evtin.Data, &tel)
			if err != nil {
				log.Fatal(err)
			}
			log.Println("tel", tel)
			// Push the event to the RabbitMQ queue
			body, err := proto.Marshal(&tel)
			if err != nil {
				log.Fatalf("Failed to marshal event: %v", err)
			}
			err = channel.Publish(queue.Name, "", false, false, amqp.Publishing{
				Body: body,
			})
			if err != nil {
				log.Fatalf("Failed to publish event to RabbitMQ: %v", err)
			}
		} else {
			log.Printf("UNKNOWN EVT type %s", evtin.Type)
		}
	}
}
