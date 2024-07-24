package main

import (
	"context"
	"encoding/csv"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v4"
)

type DeviceMessage struct {
	DeviceID    int    `json:"app_id"`
	MessageType string `json:"message_type"`
	Message     string `json:"message"`
}

func main() {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres:1234@localhost:5432/test")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(context.Background())

	router := gin.Default()

	
	router.POST("/insert", func(c *gin.Context) {
		var newMessage DeviceMessage
		if err := c.ShouldBindJSON(&newMessage); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		_, err := conn.Exec(context.Background(), "INSERT INTO device_messages (app_id, message_type, message) VALUES ($1, $2, $3)", newMessage.DeviceID, newMessage.MessageType, newMessage.Message)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"status": "Data inserted successfully"})
	})

	
	router.POST("/insert_from_csv", func(c *gin.Context) {
		file, err := os.Open("csv_file.csv")
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		reader := csv.NewReader(file)
		records, err := reader.ReadAll()
		if err != nil {
			log.Fatal(err)
		}

		for _, record := range records[1:] { 
			deviceID, _ := strconv.Atoi(record[0])
			messageType := record[1]
			message := record[2]

			_, err := conn.Exec(context.Background(), "INSERT INTO device_messages (app_id, message_type, message) VALUES ($1, $2, $3)", deviceID, messageType, message)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
		}

		c.JSON(http.StatusOK, gin.H{"status": "All data from CSV inserted successfully"})
	})

	
	router.GET("/messages", func(c *gin.Context) {
		deviceIDStr := c.Query("device_id")
		var rows pgx.Rows
		var err error

		if deviceIDStr != "" {
			deviceID, err := strconv.Atoi(deviceIDStr)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid device_id"})
				return    
			}
		rows, _ = conn.Query(context.Background(), "SELECT device_id, message_type, message FROM device_messages WHERE device_id=$1", deviceID)
		} else {
			rows, err = conn.Query(context.Background(), "SELECT device_id, message_type, message FROM device_messages")
		}

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer rows.Close()

		var messages []DeviceMessage
		for rows.Next() {
			var message DeviceMessage
			err := rows.Scan(&message.DeviceID, &message.MessageType, &message.Message)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			messages = append(messages, message)
		}

		if err = rows.Err(); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"messages": messages})
	})

	router.Run(":8081") 
}
