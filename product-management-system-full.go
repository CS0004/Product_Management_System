package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/redis/go-redis/v8"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/spf13/viper"
)

// Configuration Management
func initConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./configs")
	viper.AddConfigPath(".")

	// Read environment variables
	viper.AutomaticEnv()

	// Read config file
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}
}

// Database Migration Script
func runMigrations(db *pgxpool.Pool) error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			username VARCHAR(100) UNIQUE NOT NULL,
			email VARCHAR(100) UNIQUE NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS products (
			id SERIAL PRIMARY KEY,
			user_id INTEGER REFERENCES users(id),
			product_name VARCHAR(255) NOT NULL,
			product_description TEXT,
			product_images TEXT[],
			compressed_product_images TEXT[] DEFAULT ARRAY[]::TEXT[],
			product_price DECIMAL(10,2) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_products_user_id ON products(user_id)`,
	}

	for _, migration := range migrations {
		_, err := db.Exec(context.Background(), migration)
		if err != nil {
			return fmt.Errorf("migration error: %v", err)
		}
	}
	return nil
}

// Database Connection
func initPostgresConnection() *pgxpool.Pool {
	dbConfig := viper.GetString("database.url")
	
	config, err := pgxpool.ParseConfig(dbConfig)
	if err != nil {
		log.Fatalf("Unable to parse database config: %v", err)
	}

	config.MaxConns = viper.GetInt32("database.max_connections")
	config.MinConns = viper.GetInt32("database.min_connections")
	config.MaxConnLifetime = viper.GetDuration("database.max_conn_lifetime") * time.Minute

	pool, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}

	// Run migrations
	if err := runMigrations(pool); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	return pool
}

// Redis Connection
func initRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     viper.GetString("redis.addr"),
		Password: viper.GetString("redis.password"),
		DB:       viper.GetInt("redis.db"),
	})
}

// RabbitMQ Connection
func initRabbitMQConnection() *amqp.Connection {
	rabbitURL := viper.GetString("rabbitmq.url")
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	return conn
}

// Structured Logging Setup
func initLogger() *logrus.Logger {
	logger := logrus.New()
	
	// Set log level
	logLevel, err := logrus.ParseLevel(viper.GetString("logging.level"))
	if err != nil {
		logLevel = logrus.InfoLevel
	}
	logger.SetLevel(logLevel)

	// JSON formatting
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	})

	// Log output
	logOutput := viper.GetString("logging.output")
	switch logOutput {
	case "file":
		file, err := os.OpenFile(
			viper.GetString("logging.file_path"), 
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 
			0666,
		)
		if err == nil {
			logger.SetOutput(file)
		}
	default:
		logger.SetOutput(os.Stdout)
	}

	return logger
}

// Enhanced Product Repository with Advanced Querying
type ProductRepository struct {
	db *pgxpool.Pool
	logger *logrus.Logger
}

// Advanced Product Retrieval with Filtering
func (r *ProductRepository) GetProducts(
	ctx context.Context, 
	userID int64, 
	minPrice, maxPrice float64, 
	productName string,
) ([]Product, error) {
	query := `
		SELECT id, user_id, product_name, product_description, 
		       product_images, compressed_product_images, product_price, created_at
		FROM products
		WHERE user_id = $1
		  AND ($2 = 0 OR product_price BETWEEN $2 AND $3)
		  AND ($4 = '' OR product_name ILIKE '%' || $4 || '%')
	`

	rows, err := r.db.Query(ctx, query, userID, minPrice, maxPrice, productName)
	if err != nil {
		r.logger.WithError(err).Error("Failed to retrieve products")
		return nil, err
	}
	defer rows.Close()

	var products []Product
	for rows.Next() {
		var p Product
		err := rows.Scan(
			&p.ID, &p.UserID, &p.ProductName, &p.ProductDescription,
			&p.ProductImages, &p.CompressedProductImages, 
			&p.ProductPrice, &p.CreatedAt,
		)
		if err != nil {
			r.logger.WithError(err).Error("Failed to scan product row")
			return nil, err
		}
		products = append(products, p)
	}

	return products, nil
}

// Comprehensive Image Processing Service
type ImageProcessingService struct {
	rabbitMQConn *amqp.Connection
	s3Client     *S3Client  // Hypothetical S3 client
	pgPool       *pgxpool.Pool
	logger       *logrus.Logger
}

// Detailed Image Processing Worker
func (s *ImageProcessingService) StartImageProcessingWorker() {
	ch, err := s.rabbitMQConn.Channel()
	if err != nil {
		s.logger.WithError(err).Fatal("Failed to open channel")
		return
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"image_processing_queue", 
		true,   // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	if err != nil {
		s.logger.WithError(err).Fatal("Failed to declare queue")
		return
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		s.logger.WithError(err).Fatal("Failed to register consumer")
		return
	}

	forever := make(chan bool)

	go func() {
		for msg := range msgs {
			s.processImage(msg.Body)
			msg.Ack(false)
		}
	}()

	<-forever
}

func (s *ImageProcessingService) processImage(imageURL []byte) {
	// Comprehensive image processing logic
	url := string(imageURL)
	
	// Download image
	imageData, err := downloadImage(url)
	if err != nil {
		s.logger.WithFields(logrus.Fields{
			"image_url": url,
			"error":     err,
		}).Error("Failed to download image")
		return
	}

	// Compress image
	compressedImage, err := compressImage(imageData)
	if err != nil {
		s.logger.WithFields(logrus.Fields{
			"image_url": url,
			"error":     err,
		}).Error("Failed to compress image")
		return
	}

	// Upload to S3
	s3URL, err := s.s3Client.UploadImage(compressedImage)
	if err != nil {
		s.logger.WithFields(logrus.Fields{
			"image_url": url,
			"error":     err,
		}).Error("Failed to upload compressed image")
		return
	}

	// Update product record
	err = s.updateProductImage(url, s3URL)
	if err != nil {
		s.logger.WithFields(logrus.Fields{
			"image_url": url,
			"s3_url":    s3URL,
			"error":     err,
		}).Error("Failed to update product image")
	}
}

// Main Application Startup
func main() {
	// Initialize configuration
	initConfig()

	// Setup dependencies
	db := initPostgresConnection()
	defer db.Close()

	redisClient := initRedisClient()
	defer redisClient.Close()

	rabbitMQConn := initRabbitMQConnection()
	defer rabbitMQConn.Close()

	logger := initLogger()

	// Initialize repositories and services
	productRepo := &ProductRepository{
		db:     db,
		logger: logger,
	}

	imageProcessor := &ImageProcessingService{
		rabbitMQConn: rabbitMQConn,
		logger:       logger,
	}

	// Start image processing worker
	go imageProcessor.StartImageProcessingWorker()

	// Setup Gin router
	r := gin.New()
	r.Use(gin.Recovery())

	// Logging middleware
	r.Use(func(c *gin.Context) {
		start := time.Now()
		c.Next()
		duration := time.Since(start)

		logger.WithFields(logrus.Fields{
			"method":     c.Request.Method,
			"path":       c.Request.URL.Path,
			"status":     c.Writer.Status(),
			"latency":    duration.String(),
			"client_ip":  c.ClientIP(),
		}).Info("HTTP Request")
	})

	// API Routes
	v1 := r.Group("/api/v1")
	{
		v1.POST("/products", func(c *gin.Context) {
			// Product creation handler
		})

		v1.GET("/products", func(c *gin.Context) {
			// Products listing handler
		})

		v1.GET("/products/:id", func(c *gin.Context) {
			// Single product retrieval handler
		})
	}

	// Start server
	port := viper.GetString("server.port")
	logger.Infof("Starting server on port %s", port)
	r.Run(fmt.Sprintf(":%s", port))
}

// Sample Config (configs/config.yaml)
/*
database:
  url: postgresql://user:password@localhost:5432/productdb
  max_connections: 20
  min_connections: 5
  max_conn_lifetime: 30

redis:
  addr: localhost:6379
  password: ""
  db: 0

rabbitmq:
  url: amqp://guest:guest@localhost:5672/

logging:
  level: info
  output: stdout
  file_path: /var/log/product_service.log

server:
  port: 8080
*/
