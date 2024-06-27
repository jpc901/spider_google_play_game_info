package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"t/mongo"
	"t/postgre"

	"github.com/IBM/sarama"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type GameInfo struct {
	Id            primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	Name          string             `json:"name" bson:"name"`
	Avatar        string             `json:"avatar" bson:"avatar"`
	Company       string             `json:"company" bson:"company"`
	DownloadTimes string             `json:"download_times" bson:"download_times"`
	Description   string             `json:"description" bson:"description"`
	ApkUrl        string             `json:"apk_url" bson:"apk_url"`
}

func storeMongoDB(gameInfo *GameInfo) {
	// 指定获取要操作的数据集
	collection := mongo.MongoDBClient.Database("spider").Collection("game_info")

	insertResult, err := collection.InsertOne(context.TODO(), *gameInfo)
	if err != nil {
		log.Println("insert gameinfo to mongo failed, err:", err)
		return
	}

	fmt.Println("Inserted a single document: ", insertResult.InsertedID)
}

func storePostgre(gameInfo *GameInfo) {
	// 插入数据的SQL语句
	sqlStatement := `
    INSERT INTO game_info (name, avatar, company, download_times, description, apk_url)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING id`

	// 执行插入操作并获取新插入行的ID
	var id int64
	err := postgre.PostgreDB.QueryRow(sqlStatement, gameInfo.Name, gameInfo.Avatar, gameInfo.Company, gameInfo.DownloadTimes, gameInfo.Description, gameInfo.ApkUrl).Scan(&id)
	if err != nil {
		log.Println("insert gameinfo to postgre failed, err:", err)
		return
	}
	fmt.Printf("Inserted a single record with ID: %d\n", id)
}

// func downloadApk(gameInfo *GameInfo) {

// }

// 处理消息，下载apk并且将数据存放到 mongo & pg
func processMessage(msg []byte) error {
	gameInfo := &GameInfo{}
	if err := json.Unmarshal(msg, gameInfo); err != nil {
		log.Printf("Error occurred while unmarshaling JSON: %v", err)
		return err
	}
	storeMongoDB(gameInfo)
	storePostgre(gameInfo)
	// downloadApk(gameInfo)
	return nil
}

// 接收消息
func receiveMsg() error {
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Println("Failed to start Sarama consumer:", err)
		return err
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition("gameinfo", 0, sarama.OffsetNewest)
	if err != nil {
		log.Println("Failed to start partition consumer:", err)
		return err
	}
	defer partitionConsumer.Close()

	for msg := range partitionConsumer.Messages() {
		log.Printf("Consumed message offset %d\n", msg.Offset)
		if err := processMessage(msg.Value); err != nil {
			log.Println("processMessage failed, err:", err)
		}
	}
	return nil
}

func main() {
	if err := mongo.Init(); err != nil {
		log.Println("Init mongo Failed, err:", err)
	}
	defer func() {
		// 断开连接
		err := mongo.MongoDBClient.Disconnect(context.TODO())
		if err != nil {
			log.Println("mongo close failed", err)
		}
	}()

	if err := postgre.Init(); err != nil {
		log.Println("Init postgre Failed, err:", err)
	}
	defer func() {
		// 断开连接
		postgre.PostgreDB.Close()
		if err := postgre.PostgreDB.Close(); err != nil {
			log.Println("postgre close failed", err)
		}
	}()

	if err := receiveMsg(); err != nil {
		log.Println("receive msg err:", err)
	}
}
