package mongo

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var MongoDBClient *mongo.Client

func Init() error {
	var err error
	// 设置客户端连接配置
	clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
	// 连接到MongoDB
	MongoDBClient, err = mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	// 检查连接
	err = MongoDBClient.Ping(context.TODO(), nil)
	if err != nil {
		return err
	}
	return nil
}
