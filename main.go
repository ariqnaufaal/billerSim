package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	iso8583 "github.com/harda/iso8583"
)

// var bootstrapServer = "localhost:9092"
// var listenIP = "0.0.0.0"
// var listenPORT = "2120"
var connType = "tcp"

type IsoConfig struct {
	IsoIpAddress  string
	IsoPort       int
	KafkaAddress  string
	IsSslPort     bool
	SslService    string
	TopicRequest  string
	TopicResponse string
}

type HeaderStruct struct {
	Length int
	TPDU   string
	MTI    string
	Bitmap string
}

type MessageStruct struct {
	ISOMessage    string
	EncodeMessage string
}

var connectionMap map[string]net.Conn
var myConn *Message

func main() {

	// var l net.Listener

	// log.Println("Service started")
	// l, err = net.Listen("tcp", fmt.Sprintf(":8432"))
	// if err != nil {
	// 	fmt.Println("Error listening:", err.Error())
	// 	os.Exit(1)
	// }
	// defer l.Close()

	l, err := net.Listen("tcp", "0.0.0.0:8432")
	if err != nil {
		log.Println("Error Listening:", err.Error())
	}
	defer l.Close()
	log.Println("Listening TCP on 0.0.0.0:8432")

	connectionMap = make(map[string]net.Conn)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		defer conn.Close()

		connectionService := &ConnectionService{}

		myConn = NewConnection(conn)
		myConn.Subscribe(connectionService)

		log.Println("new connection detected !!")
		// go consumeFromKafka(c)
		// go produceToKafka(conn, p)
		go echoMessage(conn, context.Background())
	}
}

func echoMessage(conn net.Conn, ctx context.Context) {
	fmt.Println("Prepare to Echo Message")
	var dataElement map[int64]string
	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("worker maximum lifespan reached for connection: ", conn.RemoteAddr())
			return
		default:
			msg, err := readMessage(conn)
			if err != nil {
				time.Sleep(2 * time.Millisecond)
				continue
			}
			isomsg := msg.ISOMessage
			fmt.Println("message: ", isomsg)
			log.Println("message: ", isomsg)
			// dataElement := parseIsoMsg(isomsg)

			isostruct := iso8583.NewISOStruct("spec1987pos3.yml", false)
			parsed, err := isostruct.Parse(isomsg, true)
			if err != nil {
				fmt.Println("Parse record to struct error ", err)

			}
			dataElement = parsed.Elements.GetElements()

			printSortedDE(dataElement)

			mti := parsed.Mti.String()
			fmt.Println("mti: ", mti)

			isomsgEncode := msg.EncodeMessage

			//print request
			fmt.Println("Data Elements: ", dataElement)

			//select topic based on DE#3
			fmt.Println("pcode: ", dataElement[3])
			log.Println("pcode: ", dataElement[3])
			pcode, _ := strconv.Atoi(dataElement[3])

			switch pcode {
			case 000000:
				if mti == "0200" {
					fmt.Println("Transaction type: BRI Sales")
				} else if mti == "0400" {
					fmt.Println("Transaction type: BRI Reversal")
				}
			case 010000:
				if mti == "0200" {
					fmt.Println("Transaction type: BRI Cash Advance")
				} else if mti == "0400" {
					fmt.Println("Transaction type: BRI Void Cash Advance")
				}
			case 020000:
				if mti == "0200" {
					fmt.Println("Transaction type: BRI Void Sales")
				} else if mti == "0400" {
					fmt.Println("Transaction type: BRI Void Instalment")
				}
			case 310000:
				if mti == "0100" {
					fmt.Println("Transaction type: BRI Point Balance")
				}
			case 500000:
				if mti == "0200" {
					fmt.Println("Transaction type: BRI Point Inquire")
				}
			case 510000:
				if mti == "0200" {
					fmt.Println("Transaction type: BRI Point")
				} else if mti == "0320" {
					fmt.Println("Transaction type: BRI Upload Batch")
				}
			case 920000:
				if mti == "0800" {
					fmt.Println("Transaction type: BRI LTWK")
				} else if mti == "0500" {
					fmt.Println("Transaction type: BRI Settlement Header")
				}
			case 960000:
				if mti == "0500" {
					fmt.Println("Transaction type: BRI Settlement Trail")
				}
			case 970000:
				fmt.Println("Transaction type: BRI LTMK")
			default:
				fmt.Println("unknown pcode")
			}

			fmt.Println("BRI Transaction Request Transaction Detected")

			fmt.Println("isomsgEncode: ", isomsgEncode)

			connectionMap[dataElement[41]] = conn

			log.Println("send back to client: ", isomsgEncode)
			sendToClient(isomsgEncode)
		}

	}
}

func readMessage(conn net.Conn) (MessageStruct, error) {
	var messageStruct MessageStruct

	reader := bufio.NewReader(conn)

	header := make([]byte, 2)

	n, _ := reader.Read(header)

	if n == 0 {
		return messageStruct, fmt.Errorf("no message read")
	}
	fmt.Println("connection detail: ", conn.LocalAddr(), conn.RemoteAddr())

	fmt.Println("number of byte readed: ", n)
	log.Println("header length in byte: ", n)

	len1 := int(header[0])
	len2 := int(header[1])

	len1 <<= 8
	fmt.Printf("header-0 %d \n", len1)
	length := len1 + len2

	fmt.Printf("Length Message %d \n", length)
	log.Printf("Length Message %d \n", length)

	message := make([]byte, length)
	reader.Read(message)

	isomsg := string(message)

	encodeString := hex.EncodeToString(message)
	fmt.Println("encode String: ", encodeString)

	messageStruct = MessageStruct{ISOMessage: isomsg, EncodeMessage: encodeString}
	return messageStruct, nil
}

func parseIsoMsg(isomsg string) (dataElement map[int64]string) {

	isostruct := iso8583.NewISOStruct("spec1987pos3.yml", false)
	parsed, err := isostruct.Parse(isomsg, true)
	if err != nil {
		fmt.Println("Parse record to struct error ", err)

	}
	dataElement = parsed.Elements.GetElements()

	fmt.Printf("MTI: %v\n", parsed.Mti)
	// HeaderStruct{MTI: parsed.Mti}

	return dataElement
}

func printSortedDE(dataElement map[int64]string) {
	int64toSort := make([]int, 0, len(dataElement))
	for key := range dataElement {
		int64toSort = append(int64toSort, int(key))
	}
	sort.Ints(int64toSort)
	for _, key := range int64toSort {
		fmt.Printf("[%v] : %v\n", int64(key), dataElement[int64(key)])
	}
}

func responseMessage(conn net.Conn, isomsg string) {

	msgHex := []byte(isomsg)

	lenbyte := make([]byte, 2)
	lenbyte[0] = byte(len(msgHex) / 256)
	lenbyte[1] = byte(len(msgHex))
	fmt.Printf("response message, header length: %#v\n", lenbyte)
	fmt.Println("connection detail: ", conn.LocalAddr(), conn.RemoteAddr())

	//	conn.Write(lenbyte)
	//	conn.Write(msgHex)
	msgSend := append(lenbyte, msgHex...)
	conn.Write(msgSend)

}

func sendToClient(isomsgEncode string) {

	var dataElement map[int64]string

	for {
		//read the information from the message.
		rawmsg := string(isomsgEncode)
		msgHex, _ := hex.DecodeString(rawmsg)
		fmt.Println("Received record from kafka : ", rawmsg)

		isostruct := iso8583.NewISOStruct("spec1987pos3.yml", false)
		parsed, err := isostruct.Parse(string(msgHex), true)
		if err != nil {
			fmt.Println("Parse record to struct error ", err)

		}
		dataElement = parsed.Elements.GetElements()

		printSortedDE(dataElement)

		mti := parsed.Mti.String()
		fmt.Println("mti: ", mti)
		//print response
		fmt.Println("Data Elements (to be saved): ", dataElement)

		int64toSort := make([]int, 0, len(dataElement))
		for key := range dataElement {
			int64toSort = append(int64toSort, int(key))
		}
		sort.Ints(int64toSort)
		for _, key := range int64toSort {
			fmt.Printf("[%v] : %v\n", int64(key), dataElement[int64(key)])
		}
		fmt.Println("End of message..")

		// responseMessage(string(msgHex))
		myConn.Fire(string(msgHex))
	}

}
