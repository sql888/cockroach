// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/magiconair/properties"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

// TODO(gli): This recoverfromcdc is to recover from the last successful backup to the point of the crash via CDC. For now, only CDC to Kafka is supported. This can be extended to other sinks.

var recoverfromcdcCmd = &cobra.Command{
	Use:   "recoverfromcdc",
	Short: "Recover Cluster from CDC (Kafka)",
	Long:  "Recover Cluster from CDC (Kafka) after restoring from last successful backup, so it can recover to the point of failure.",
	RunE:  runRecoverFromCDC,
}

const (
	// BatchTimeout is the number of milliseconds to force to send the batch. in case of a slow consumer, which could take a long time to reach the batch size
	BatchTimeout = 5000 * time.Millisecond
	// ReportIntervalSeconds is the number of seconds to report the progress of the recovery  for each partition.
	ReportIntervalSeconds = 10
	// KafkaConsumerMaxMessageBytes is the message bytes for the consumer to fetch
	KafkaConsumerMaxMessageBytes = 33554432
	// CDCKeyAfter is the after image of the table row. This can be used to determine what type of the message.  Upsert or Delete
	CDCKeyAfter = "after"
	// CDCKeyBefore is the before image of the table row. This can be used to determine what type of the message.  Upsert or Delete
	CDCKeyBefore = "before"
	// CDCKeyResolved is the special type of message. currently this will be ignored.
	CDCKeyResolved = "resolved"
	// CDCKeyUpdated is the timestamp in seconds with decimal (nanosecond precision).  This can be parsed, then filter/skip from the point-in-time
	CDCKeyUpdated = "updated"
	// CDCMessageTypeUpsert is the message type for upsert
	CDCMessageTypeUpsert = "upsert"
	// CDCMessageTypeDelete is the message type for delete
	CDCMessageTypeDelete = "delete"
	// CDCMessageTypeUpdated is the message type for the update timestamp only, this is currently being ignored/skipped
	CDCMessageTypeUpdated = "updated"
	// CDCMessageTypeResolved is the message type for the resolved timestamp only, this is currently being ignored/skipped
	CDCMessageTypeResolved = "resolved"
	// CDCMessageTypeUnknown is the unknown message type
	CDCMessageTypeUnknown = "unknown"
	// KafkaStartOffsetDefault is the default value when it's not specified in the CLI
	KafkaStartOffsetDefault = 0.0
	// KafkaEndOffsetDefault is the default value when it's not specified in the CLI
	KafkaEndOffsetDefault = -1.0
	// EmptyString is the empty string ""
	EmptyString = ""
	// RecoverBatchSizeDefault is the number of message to consume, de-dup, and upsert/delete.
	RecoverBatchSizeDefault = 100
	// RecoverCockroachDBNameDefault is the default database name
	RecoverCockroachDBNameDefault = "system"
	// RecoverStartTimestampDefault is the default UTC date/time of the recovery time
	RecoverStartTimestampDefault = "1970-01-01T00:00:00.000000000Z" // UTC time
	// CDCInternalColumnPrefix is the prefix of crdb internal column name that should be skipped. For tables with a hash-sharded index, there is an extra column called `crdb_internal_xxx_shard_n`
	CDCInternalColumnPrefix = "crdb_internal"
	// DBErrorRetriesDefault is the max number of retries when database error is encountered for delete/upsert
	DBErrorRetriesDefault = 3
	// RecoverFromCDCAppName is the AppName used to connect to the CRDB
	RecoverFromCDCAppName = "recoverfromcdc"
	// DBConnectionMinRetrySleepSeconds is the minimum number of seconds it sleep between retries
	DBConnectionMinRetrySleepSeconds = 1
	// DBConnectionMaxRetrySleepSeconds is the maximum number of seconds to sleep between retries
	DBConnectionMaxRetrySleepSeconds = 60
	// ReportSectionDivider is the section divider for pretty printing
	ReportSectionDivider = "============================================================================================================="
	// KafkaParamBootstrapServers is the property bootstrap.servers in the property file. this can be overridden by the CLI
	KafkaParamBootstrapServers = `bootstrap.servers`
	// KafkaParamTopicName is the property topic.name in the property file. this can be overridden by the CLI
	KafkaParamTopicName = `topic.name`
	// KafkaParamSASLMechanism is the property name for sasl.mechanism. e.g. PLAIN, GSSAPI
	KafkaParamSASLMechanism = `sasl.mechanism`
	// KafkaParamSecurityProtocol is the property name for security.protocol. e.g. SASL_SSL, SASL_PLAINTEXT, PLAINTEXT
	KafkaParamSecurityProtocol = `security.protocol`
	// KafkaParamClientID is the client id of the producer/consumer,  if this is not specified in the property file, it will be generated using CockroachDBRecoverFromCDC-<KafkaTopicName>
	KafkaParamClientID = `client.id`
	// KafkaParamSASLJAASConfig is the sasl.jaas.config in the property file, it has the username, password.
	KafkaParamSASLJAASConfig = `sasl.jaas.config`
	// KafkaParamSSLTrustStoreType is ssl.truststore.type, e.g. PEM
	KafkaParamSSLTrustStoreType = `ssl.truststore.type`
	// KafkaParamSSLTrustStoreLocation is the file location of the RootCA.
	KafkaParamSSLTrustStoreLocation = `ssl.truststore.location`
	// KafkaClientTypeConsumer is the kafka client type Consumer
	KafkaClientTypeConsumer = `Consumer`
	// KafkaClientTypeProducer is the kafka client type Producer
	KafkaClientTypeProducer = `Producer`
)

// runRecoverFromCDC implements to logic to recover the db.table using CDC, only Kafka sink is supported as of now.
func runRecoverFromCDC(cmd *cobra.Command, args []string) (resErr error) {
	var conn clisqlclient.Conn
	var dbConnPools map[int32]map[string]*pgxpool.Pool
	var saramaProducerConfig *sarama.Config
	var kafkaProducerBootstrapServers, kafkaProducerTopicName string
	var pkColumns []string

	// RecoverKafkaBootstrapServer & RecoverKafkaTopicName can be from the property file if not specified via the CLI
	//if len(recoverfromcdcCtx.RecoverKafkaBootstrapServer) == 0 {
	//	return errors.Errorf("RecoverKafkaBootstrapServer: %v is empty", recoverfromcdcCtx.RecoverKafkaBootstrapServer)
	//}

	//if len(recoverfromcdcCtx.RecoverKafkaTopicName) == 0 {
	//	return errors.Errorf("RecoverKafkaTopicName: %v is empty", recoverfromcdcCtx.RecoverKafkaTopicName)
	//}

	recoverStartTimestampEpochNanoSeconds := KafkaStartOffsetDefault
	if len(recoverfromcdcCtx.RecoverStartTimestamp) > 0 {
		// convert the UTC timestamp to Unix epoch nanoseconds as float64
		recoverStartTime, err := time.Parse(time.RFC3339Nano, recoverfromcdcCtx.RecoverStartTimestamp)
		if err != nil {
			return errors.Wrapf(err, "invalid recoverfromcdcCtx.RecoverStartTimestamp: %v", recoverfromcdcCtx.RecoverStartTimestamp)
		}
		recoverStartTimestampEpochNanoSeconds = float64(recoverStartTime.UnixNano())
	}

	recoverEndTimestampEpochNanoSeconds := KafkaEndOffsetDefault
	if len(recoverfromcdcCtx.RecoverEndTimestamp) > 0 {
		// convert the UTC timestamp to Unix epoch nanoseconds as float64
		recoverEndTime, err := time.Parse(time.RFC3339Nano, recoverfromcdcCtx.RecoverEndTimestamp)
		if err != nil {
			return errors.Wrapf(err, "invalid recoverfromcdcCtx.RecoverEndTimestamp: %v", recoverfromcdcCtx.RecoverEndTimestamp)
		}
		recoverEndTimestampEpochNanoSeconds = float64(recoverEndTime.UnixNano())
	}

	ctx := context.Background()

	saramaConfig, kafkaBootstrapServers, kafkaTopicName, err := NewSaramaConfig(recoverfromcdcCtx.RecoverKafkaCommandConfigFile, recoverfromcdcCtx.RecoverKafkaBootstrapServer, recoverfromcdcCtx.RecoverKafkaTopicName, KafkaClientTypeConsumer)
	if err != nil {
		return errors.Wrapf(err, "Error from NewSaramaConfig for consumer: %v", recoverfromcdcCtx.RecoverKafkaCommandConfigFile)
	}

	sarama.Logger = log.New(os.Stdout, "sarama: ", log.Llongfile)

	fmt.Printf("saramaConfig: %v\n", saramaConfig)
	fmt.Printf("saramaConfig.Net.SASL.Enable: %v\n", saramaConfig.Net.SASL.Enable)
	kafkaClient, err := sarama.NewConsumer(strings.Split(kafkaBootstrapServers, ","), saramaConfig)
	if err != nil {
		return errors.Wrapf(err, "Error creating a new consumer: %v", kafkaBootstrapServers)
	}

	topicPartitions, err := getPartitions(kafkaClient)
	if err != nil {
		return errors.Wrapf(err, "Error getting partitions from kafka cluster: %v, topic: %v", kafkaBootstrapServers, kafkaTopicName)
	}

	// There are 2 modes of recoverfromcdc.  one for replaying the Kafka topic, one for create a fan out pipe to another Kafka topic with more partitions for higher throughput/parallelism.
	// when RecoverKafkaFanOutEnable is specified in the command line, it's in the fan-out mode.
	// There maybe many 3rd-party tools to replicate kafka topic.  However, for convenience as a producer offering and
	// also to ensure the data ordering is preserve on the original message key, which is the primary key column(s), it's better to provide such tooling.
	// The high fan out can be used to create benchmark loads with higher concurrency as well, which optionally can turn off de-dup of the original messages.
	if recoverfromcdcCtx.RecoverKafkaFanOutEnable {
		saramaProducerConfig, kafkaProducerBootstrapServers, kafkaProducerTopicName, err = NewSaramaConfig(recoverfromcdcCtx.RecoverKafkaFanOutTargetCommandConfigFile, recoverfromcdcCtx.RecoverKafkaFanOutTargetBootstrapServer, recoverfromcdcCtx.RecoverKafkaFanOutTargetTopicName, KafkaClientTypeProducer)
		if err != nil {
			return errors.Wrapf(err, "Error from NewSaramaConfig for producer: %v", recoverfromcdcCtx.RecoverKafkaFanOutTargetCommandConfigFile)
		}
	} else {
		// Validate the CLI parameters
		if len(recoverfromcdcCtx.RecoverCockroachDBName) == 0 {
			return errors.Errorf("RecoverCockroachDBName: %v is empty", recoverfromcdcCtx.RecoverCockroachDBName)
		}

		if len(recoverfromcdcCtx.RecoverCockroachTableName) == 0 {
			return errors.Errorf("RecoverCockroachTableName: %v is empty", recoverfromcdcCtx.RecoverCockroachTableName)
		}

		conn, err = makeSQLClient(RecoverFromCDCAppName, useSystemDb)
		if err != nil {
			return err
		}
		defer func() { resErr = errors.CombineErrors(resErr, conn.Close()) }()

		isValid, err := validateDBName(ctx, conn, recoverfromcdcCtx.RecoverCockroachDBName)
		if err != nil {
			return errors.Wrapf(err, "Error validating RecoverCockroachDBName: %v", recoverfromcdcCtx.RecoverCockroachDBName)
		}
		if !isValid {
			return errors.Errorf("RecoverCockroachDBName: %v does not exist.", recoverfromcdcCtx.RecoverCockroachDBName)
		}

		isValid, err = validateTableName(ctx, conn, recoverfromcdcCtx.RecoverCockroachDBName, recoverfromcdcCtx.RecoverCockroachTableName)
		if err != nil {
			return errors.Wrapf(err, "Error validating RecoverCockroachTableName: %v", recoverfromcdcCtx.RecoverCockroachTableName)
		}
		if !isValid {
			return errors.Errorf("RecoverCockroachTableName: %v does not exist.", recoverfromcdcCtx.RecoverCockroachTableName)
		}

		// Get the PK columns of the table. This is used for Delete.
		// Currently, it assumes the PK is not changed during the recovery.
		// TODO: support PK column(s) change, e.g. detect the Kafka Message `Key` combination is changed. replay Kafka message in 2 phases, one with an ending offset for the old PK,  and then starting new offset when the PK is modified.
		pkColumns, err = getTablePKColumns(ctx, conn, recoverfromcdcCtx.RecoverCockroachDBName, recoverfromcdcCtx.RecoverCockroachTableName)
		if err != nil {
			return errors.Wrapf(err, "Error getting Primary Key columns for RecoverCockroachTableName: %v", recoverfromcdcCtx.RecoverCockroachTableName)
		}
		fmt.Printf("pkColumns: %v\n", pkColumns)

		// For the replay mode, make the connection pool to the cockroachdb
		dbConnPools, err = GetDBConnectionPool(ctx, conn, topicPartitions, recoverfromcdcCtx.RecoverUseBalanceDBConnection, recoverfromcdcCtx.RecoverUseBalanceDBConnectionLocalityFilter)
		if err != nil {
			return errors.Wrapf(err, "Failed to get dbConnPools for topicPartitions: %v\n", topicPartitions)
		}
		defer cleanupDBConnPools(dbConnPools)
	}

	var (
		messages = make(chan *sarama.ConsumerMessage, recoverfromcdcCtx.RecoverBatchSize)
		closing  = make(chan struct{})
	)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, os.Interrupt)
		<-signals
		// Note: sending SIGINT (kill -INT) to the process id works.  CTRL+C not always working. See similar issue reported in https://github.com/go-task/task/issues/458
		fmt.Printf("Initiate the shutdown of consumers...\n")
		close(closing)
	}()

	pcGroup, pcCtx := errgroup.WithContext(ctx)
	pcCtx, cancel := context.WithCancel(pcCtx)
	defer cancel()

	for _, partition := range topicPartitions {
		// closures with goroutines concurrency
		partition := partition
		fmt.Printf("partition: %v\n", partition)
		partitionStartOffset := sarama.OffsetOldest
		if recoverfromcdcCtx.RecoverKafkaStartOffset > KafkaStartOffsetDefault {
			partitionStartOffset = recoverfromcdcCtx.RecoverKafkaStartOffset
		}
		pc, err := kafkaClient.ConsumePartition(kafkaTopicName, partition, partitionStartOffset)
		if err != nil {
			return errors.Wrapf(err, "Failed to start consumer for partition %d", partition)
		}

		go func(pc sarama.PartitionConsumer, partition int32) {
			<-closing
			fmt.Printf("Closing the consumer for partition: %v\n", partition)
			pc.AsyncClose()
		}(pc, partition)

		//go func(pcCtx context.Context, pc sarama.PartitionConsumer, partition int32, batchSize int, conn clisqlclient.Conn, closing chan struct{}) {
		pcGroup.Go(func() error {
			err := fetchPartitionConsumer(pcCtx, pc, partition, recoverfromcdcCtx.RecoverBatchSize, dbConnPools[partition], closing, recoverStartTimestampEpochNanoSeconds, recoverEndTimestampEpochNanoSeconds, pkColumns, saramaProducerConfig, kafkaProducerBootstrapServers, kafkaProducerTopicName, recoverfromcdcCtx.RecoverKafkaFanOutEnable)
			return err
		})
	}

	// Wait for all partition consumers to complete, check the error.
	if err := pcGroup.Wait(); err != nil {
		fmt.Printf("pcGroup encourters an error: %v\n", err)
	}
	fmt.Println("Done consuming topic", kafkaTopicName)
	close(messages)

	if err := kafkaClient.Close(); err != nil {
		return errors.Wrapf(err, "Failed to close consumer: \n")
	}

	return nil
}

// validateDBName checks whether the database exists in the CRDB cluster.
func validateDBName(ctx context.Context, conn clisqlclient.Conn, dbName string) (bool, error) {
	rows, err := conn.Query(ctx,
		"SELECT 1 from crdb_internal.databases where name=$1",
		dbName)
	if err != nil {
		return false, err
	}
	row := make([]driver.Value, 1)
	if err := rows.Next(row); err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, err
	}

	if err := rows.Close(); err != nil {
		return false, err
	}

	isValid := row[0].(int64)
	if len(row) == 0 || isValid != 1 {
		return false, nil
	}

	return true, nil
}

// validateTableName checks whether the table exists in the CRDB cluster.
func validateTableName(ctx context.Context, conn clisqlclient.Conn, dbName string, tableName string) (bool, error) {
	rows, err := conn.Query(ctx,
		"SELECT 1 from crdb_internal.tables where name=$1 and database_name=$2",
		tableName,
		dbName)
	if err != nil {
		return false, err
	}
	row := make([]driver.Value, 1)
	if err := rows.Next(row); err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, err
	}

	if err := rows.Close(); err != nil {
		return false, err
	}

	isValid := row[0].(int64)
	if len(row) == 0 || isValid != 1 {
		return false, nil
	}

	return true, nil
}

// getTablePKColumns looks up the PK columns of the database.table from the CRDB/Postgres data dictionary.
// The PK columns will be used for de-duplication for a batch of Kafka messages.
func getTablePKColumns(ctx context.Context, conn clisqlclient.Conn, dbName string, tableName string) ([]string, error) {
	var pkColumns []string
	// double-quote database_name for the mix-case scenario
	pkColumnsQuery := fmt.Sprintf("select b.column_name from \"%v\".information_schema.table_constraints a join \"%v\".information_schema.constraint_column_usage b on a.constraint_name=b.constraint_name and a.table_name=b.table_name where a.constraint_type='PRIMARY KEY' and a.table_name='%v'", dbName, dbName, tableName)
	_, rows, err := sqlExecCtx.RunQuery(
		ctx,
		conn,
		clisqlclient.MakeQuery(pkColumnsQuery),
		false, /* showMoreChars */
	)

	if err != nil {
		return pkColumns, err
	}

	for _, columnName := range rows {
		pkColumns = append(pkColumns, columnName[0])
	}
	return pkColumns, nil
}

// getPartitions returns the partitions of the Kafka topic
func getPartitions(c sarama.Consumer) ([]int32, error) {
	// if no partitions specified, then get all the partitions of the topic
	if len(recoverfromcdcCtx.RecoverKafkaTopicPartitions) == 0 {
		return c.Partitions(recoverfromcdcCtx.RecoverKafkaTopicName)
	}

	partitionStrings := strings.Split(recoverfromcdcCtx.RecoverKafkaTopicPartitions, ",")
	var partitions []int32
	for i := range partitionStrings {
		val, err := strconv.ParseInt(partitionStrings[i], 10, 32)
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, int32(val))
	}

	return partitions, nil
}

// fetchPartitionConsumer fetches messages from by partition, by batch and de-dup by the message key
func fetchPartitionConsumer(pcCtx context.Context, pc sarama.PartitionConsumer, partition int32, batchSize int, conns map[string]*pgxpool.Pool, closing chan struct{}, recoverStartTimestampEpochNanoSeconds float64, recoverEndTimestampEpochNanoSeconds float64, pkColumns []string, saramaProducerConfig *sarama.Config, kafkaProducerBootstrapServers, kafkaProducerTopicName string, enableFanOut bool) error {
	var kafkaProducerClient sarama.SyncProducer
	var err error
	var currentOffset, rowsUpserted, rowsDeleted, previousRowsUpserted, previousRowsDeleted, queriesUpserted, queriesDeleted, previousQueriesUpserted, previousQueriesDeleted int64
	var msgType string
	var rowImage map[string]interface{}
	var updatedTimestamp float64
	rowsUpserted, rowsDeleted, previousRowsUpserted, previousRowsDeleted, queriesUpserted, queriesDeleted, previousQueriesUpserted, previousQueriesDeleted = 0, 0, 0, 0, 0, 0, 0, 0
	fmt.Printf("consuming pc.Messages() for partition: %v, batchSize: %v\n", partition, batchSize)
	msgBatch := make(map[string]map[string]map[string]interface{})
	rawMsgBatch := make(map[string]string)
	count := 0
	duplicateCount := 0

	// fan out mode
	if enableFanOut {
		// Use SyncProducer by default for replaying, so it won't have data loss. optionally turn on acks=-1 (all) for AtLeastOnce delivery, the performance will be slower. the producer can be parallelized by the # of partitions of the original message. and also via batching.
		// TODO(gli): for benchmark load test of using a higher fan out kafka topic, it can AsyncProducer
		kafkaProducerClient, err = sarama.NewSyncProducer(strings.Split(kafkaProducerBootstrapServers, ","), saramaProducerConfig)
		if err != nil {
			return errors.Wrapf(err, "Error creating a new producer: %v", kafkaProducerBootstrapServers)
		}
		defer func() { err = kafkaProducerClient.Close() }()
	}

	// set the 1-second timeout for now or the batchSize, which ever is reached first.
	batchTimeoutTicker := time.NewTicker(BatchTimeout)
	reportTicker := time.NewTicker(ReportIntervalSeconds * time.Second)
	defer func() {
		batchTimeoutTicker.Stop()
		reportTicker.Stop()
	}()
	var sendBatchNow = false
	for {
		select {
		case <-pcCtx.Done():
			fmt.Printf("pcCtx.Done(), exiting the goroutine for partition: %v\n", partition)
			return nil
		case <-closing:
			fmt.Printf("receive closing signal, exiting the goroutine for partition: %v\n", partition)
			return nil
		case <-batchTimeoutTicker.C:
			// if after the timeout (e.g. slow consumer), force it to write to the DB.
			sendBatchNow = true
		case <-reportTicker.C:
			// TODO(gli): have a function for report, these also can be published as metrics via cockroach's HTTP prometheus endpoint: /_status/vars
			//fmt.Printf("%s\n", ReportSectionDivider)
			updatedMVCCTime := time.Unix(int64(updatedTimestamp/1000000000), int64(updatedTimestamp)%1000000000).UTC()

			upsertRowsPerSecond := int((rowsUpserted - previousRowsUpserted) / ReportIntervalSeconds)
			previousRowsUpserted = rowsUpserted
			deleteRowsPerSecond := int((rowsDeleted - previousRowsDeleted) / ReportIntervalSeconds)
			previousRowsDeleted = rowsDeleted

			upsertQueriesPerSecond := int((queriesUpserted - previousQueriesUpserted) / ReportIntervalSeconds)
			previousQueriesUpserted = queriesUpserted
			deleteQueriesPerSecond := int((queriesDeleted - previousQueriesDeleted) / ReportIntervalSeconds)
			previousQueriesDeleted = queriesDeleted

			fmt.Printf("%v Report progress every %v seconds. for partition: %v, Current Offset: %v, High Water Mark: %v, Remaining offsets: %v, updatedTimstamp epoch nanoseconds: %f, updateMVCCTime: %v, Time to catchup: %v, Total messages consumed: %v, duplicated: %v, upserted: %v, deleted: %v, throughput(rows/second), upsert: %v, delete : %v, QPS, upsert: %v, delete: %v\n", time.Now().UTC().Format(time.RFC3339), ReportIntervalSeconds, partition, currentOffset, pc.HighWaterMarkOffset(), pc.HighWaterMarkOffset()-currentOffset, updatedTimestamp, updatedMVCCTime, time.Now().UTC().Sub(updatedMVCCTime), count, duplicateCount, rowsUpserted, rowsDeleted, upsertRowsPerSecond, deleteRowsPerSecond, upsertQueriesPerSecond, deleteQueriesPerSecond)
			//fmt.Printf("Total messages consumed: %v, duplicated: %v, upserted: %v, deleted: %v, throughput(rows/second), upsert: %v, delete : %v, QPS, upsert: %v, delete: %v\n", count, duplicateCount, rowsUpserted, rowsDeleted, upsertRowsPerSecond, deleteRowsPerSecond, upsertQueriesPerSecond, deleteQueriesPerSecond)
			fmt.Printf("%s\n", ReportSectionDivider)
		case pcErr := <-pc.Errors():
			fmt.Printf("partition: %v consumer error: %v\n", partition, pcErr)
			return errors.Wrapf(pcErr, "partition: %v consumer error", partition)
		case message := <-pc.Messages():
			// fetch one message within the for-select.  may optimize with multiple messages fetch with:  for message := range pc.Messages() {}
			currentOffset = message.Offset
			//fmt.Printf("message, Partition: %v\nOffset: %v, HighWaterMark: %v\nkey: %s\nvalue: %s\n", message.Partition, message.Offset, pc.HighWaterMarkOffset(), message.Key, message.Value)
			count++
			var jsonMap map[string]interface{}
			// The default convert to float64 using err := json.Unmarshal(message.Value, &jsonMap)
			decoderString := json.NewDecoder(strings.NewReader(string(message.Value)))
			decoderString.UseNumber()
			err := decoderString.Decode(&jsonMap)
			// fmt.Printf("jsonMap: %v\n", jsonMap)
			if err != nil {
				fmt.Printf("Error parsing mesage: %v in partition: %v, Exiting this goroutine\n", message.Value, partition)
				return errors.Wrapf(err, "Error parsing mesage: %v in partition: %v, Exiting this goroutine\n", message.Value, partition)
			}
			msgType, rowImage, updatedTimestamp, err = parseCDCMessage(jsonMap)
			if err != nil {
				fmt.Printf("Error parsing mesage: %s in partition: %v, Exiting this goroutine\n", message.Value, partition)
				return errors.Wrapf(err, "Error parsing mesage: %s in partition: %v, Exiting this goroutine\n", message.Value, partition)
			}

			// Since within the batchSize window, there could be duplicated Message Key,
			// instead of using the condition (count % batchSize !=0),  use (len(msgBatch) < batchSize)
			if (len(msgBatch) < batchSize) && !filterCDCMessage(msgType, recoverfromcdcCtx.RecoverKafkaStartOffset, recoverfromcdcCtx.RecoverKafkaEndOffset, recoverStartTimestampEpochNanoSeconds, recoverEndTimestampEpochNanoSeconds, currentOffset, updatedTimestamp) {
				//fmt.Printf("rowImage: %v, updatedTimestamp: %f\n", rowImage, updatedTimestamp)
				// This only for duplicateCount metric, the extra lookup should not incur too much performance penality
				if _, ok := msgBatch[string(message.Key)]; ok {
					duplicateCount++
				}
				// This will de-dup using the Message.Key. usually this is the PK column(s).
				msgBatch[string(message.Key)] = map[string]map[string]interface{}{msgType: rowImage}
				// preserve original message.Value for Fan Out
				rawMsgBatch[string(message.Key)] = string(message.Value)
				// the bucket reached the batchSize, set it to
				if len(msgBatch) == batchSize {
					sendBatchNow = true
				}
			}
		}

		//fmt.Printf("sendBatchNow: %v, msgBatch: %v, len(msgBaatch): %v\n", sendBatchNow, msgBatch, len(msgBatch))
		if sendBatchNow {
			sendBatchNow = false
			// if the batch has at least one record.
			if len(msgBatch) > 0 {
				batchRowsUpserted, batchRowsDeleted, batchQueriesUpserted, batchQueriesDeleted, err := processBatch(pcCtx, partition, conns, msgBatch, closing, pkColumns, rawMsgBatch, kafkaProducerClient, kafkaProducerTopicName, enableFanOut)
				rowsUpserted += int64(batchRowsUpserted)
				rowsDeleted += int64(batchRowsDeleted)
				queriesUpserted += int64(batchQueriesUpserted)
				queriesDeleted += int64(batchQueriesDeleted)
				if err != nil {
					fmt.Printf("Error writing to the DB after retries for partition: %v, currentOffset: %v, Exiting this goroutine\n", partition, currentOffset)
					return errors.Wrapf(err, "Error writing to the DB after retries for partition: %v, currentOffset: %v, Exiting this goroutine", partition, currentOffset)
				}
				// make a new map, the old one with no reference should be garbage-collected. Go 1.21 has clear(map)
				msgBatch = make(map[string]map[string]map[string]interface{})
				rawMsgBatch = make(map[string]string)
				// reset the batchTimeoutTicker
				batchTimeoutTicker.Stop()
				select {
				case <-batchTimeoutTicker.C:
				default:
				}
				batchTimeoutTicker.Reset(BatchTimeout)
			}
		}
	}
}

// processBatch processes the data as upsert and delete and submit the query to the DB.
// It will retry 3 times (to-do to parameterize this) if errors are found.
func processBatch(ctx context.Context, partition int32, conns map[string]*pgxpool.Pool, data map[string]map[string]map[string]interface{}, closing chan struct{}, pkColumns []string, rawMsgBatch map[string]string, kafkaProducerClient sarama.SyncProducer, kafkaProducerTopicName string, enableFanOut bool) (int, int, int, int, error) {
	// dataDelete holds all the delete messages beforeImage to get the list of PK Column(s)
	var dataDelete = make(map[string]map[string]interface{})
	// dataUpsert holds all the afterImage of the rows to be inserted/updated
	var dataUpsert = make(map[string]map[string]interface{})
	var producerMessages []*sarama.ProducerMessage
	upsertQueryCount, deleteQueryCount := 0, 0

	// separate the Upsert & Delete Batch, they can be executed in parallel against the CockroachDB, since the ordering in the same partition and de-dup on the same Msg Key -> PK column(s)
	for msgKey, msgData := range data {
		for msgType, rowImageMap := range msgData {
			//rowImageMap, err := convertInterfaceToMap(rowImage)
			if msgType == CDCMessageTypeUpsert {
				dataUpsert[msgKey] = rowImageMap
				continue
			}
			dataDelete[msgKey] = rowImageMap
		}
	}

	// Fan out mode,  after producing the messages,  short circuit without writing to the cockroachdb
	if enableFanOut {
		for msgKey, msgData := range rawMsgBatch {
			producerMessages = append(producerMessages, &sarama.ProducerMessage{
				Topic: kafkaProducerTopicName,
				Key:   sarama.StringEncoder(msgKey),
				Value: sarama.StringEncoder(msgData),
			})
		}
		if err := kafkaProducerClient.SendMessages(producerMessages); err != nil {
			return len(dataUpsert), len(dataDelete), upsertQueryCount, deleteQueryCount, errors.Wrapf(err, "error producing messages: %v", producerMessages)
		}
		return len(dataUpsert), len(dataDelete), upsertQueryCount, deleteQueryCount, nil
	}

	// execute the upsert/delete in parallel
	// TODO:  it's better to have separate database connections for upsert/delete and also utilize all gateway nodes in the cluster optionally filtered by the region/locality
	dbExecuteGroup, dbExecuteCtx := errgroup.WithContext(ctx)
	if len(dataUpsert) > 0 {
		dbExecuteGroup.Go(func() error {
			err := doUpsert(dbExecuteCtx, conns[CDCMessageTypeUpsert], recoverfromcdcCtx.RecoverCockroachDBName, recoverfromcdcCtx.RecoverCockroachTableName, closing, dataUpsert)
			if err != nil {
				fmt.Printf("Error dbUpert(), err: %v, dataUpsert: %v\n", err, dataUpsert)
			}
			return err
		})
		upsertQueryCount = 1
	}
	if len(dataDelete) > 0 {
		dbExecuteGroup.Go(func() error {
			err := doDelete(dbExecuteCtx, conns[CDCMessageTypeDelete], recoverfromcdcCtx.RecoverCockroachDBName, recoverfromcdcCtx.RecoverCockroachTableName, closing, pkColumns, dataDelete)
			if err != nil {
				fmt.Printf("Error dbDelete(), err: %v, dataDelete: %v\n", err, dataDelete)
			}
			return err
		})
		deleteQueryCount = 1
	}

	// Wait for all partition consumers to complete, check the error.
	if err := dbExecuteGroup.Wait(); err != nil {
		fmt.Printf("dbExecuteGroup encounters an error: %v", err)
		return 0, 0, 0, 0, errors.Wrapf(err, "dbExecuteGroup encounters an error")
	}
	return len(dataUpsert), len(dataDelete), upsertQueryCount, deleteQueryCount, nil
}

// parseCDCMessage parses the CDC message jsonMap. and returns the type of the message. See https://www.cockroachlabs.com/docs/stable/changefeed-messages
// if it has "after",  then return ("upsert", after_image, updated_timestamp_echo_nanoseconds, nil)
// if it has "before" only, no "after", then return ("delete", before_image, updated_timestamp_echo_nanoseconds, nil)
// if it has the "resolved" key, then return  ("resolved", nil, 0, nil)
// if it has the "updated" key only,  then return ("updated", nil, updated_timestamp_echo_nanoseconds, nil)
// else return ("unknown", nil, 0.0, error)
func parseCDCMessage(jsonMap map[string]interface{}) (string, map[string]interface{}, float64, error) {
	afterImage, ok := jsonMap[CDCKeyAfter]
	if ok && afterImage != nil {
		updateTimestampFloat, err := parseCDCMessageUpdatedTimestamp(jsonMap)
		if err != nil {
			return CDCMessageTypeUnknown, nil, updateTimestampFloat, err
		}
		afterImageMap, err := convertInterfaceToMap(afterImage)
		if err != nil {
			return CDCMessageTypeUnknown, nil, 0.0, err
		}
		// This short circuits the other types, the upsert includes insert with only after image), and update with both before/after image.
		return CDCMessageTypeUpsert, afterImageMap, updateTimestampFloat, nil
	}

	beforeImage, ok := jsonMap[CDCKeyBefore]
	// The delete message is like this: {"after": null, "before": {....}}
	if ok && afterImage == nil {
		updateTimestampFloat, err := parseCDCMessageUpdatedTimestamp(jsonMap)
		if err != nil {
			return CDCMessageTypeUnknown, nil, updateTimestampFloat, err
		}
		beforeImageMap, err := convertInterfaceToMap(beforeImage)
		if err != nil {
			return CDCMessageTypeUnknown, nil, 0.0, err
		}
		return CDCMessageTypeDelete, beforeImageMap, updateTimestampFloat, nil
	}

	_, ok = jsonMap[CDCKeyUpdated]
	if ok {
		// no need to parse the updated, as the updated only message will be ignored/skipped.
		// change this logic if it's needed to debug the updated only type of messages.
		return CDCMessageTypeUpdated, nil, 0.0, nil
	}

	_, ok = jsonMap[CDCKeyResolved]
	if ok {
		// no need to parse the resolved, as it will be ignored/skipped.
		// change this logic if it's needed to debug the resolved type of messages.
		return CDCMessageTypeResolved, nil, 0.0, nil
	}

	return CDCMessageTypeUnknown, nil, 0.0, errors.Errorf("unknown message type")
}

// parseCDCMessageUpdatedTimestamp parses the CDC message's "updated" key and convert it to float64/decimal
func parseCDCMessageUpdatedTimestamp(jsonMap map[string]interface{}) (float64, error) {
	updateTimestamp, ok := jsonMap[CDCKeyUpdated]
	if !ok {
		return 0.0, errors.Errorf("updated not enabled as CDC option, not supported")
	}
	updateTimestampString, ok := updateTimestamp.(string)
	if !ok {
		return 0.0, errors.Errorf("updated not string: %v", updateTimestamp)
	}

	updateTimestampFloat, err := strconv.ParseFloat(updateTimestampString, 64)
	if err != nil {
		return 0.0, errors.Wrapf(err, "updated string to float conversion error: %s", updateTimestamp)
	}
	return updateTimestampFloat, nil
}

// filterCDCMessage returns true if the CDCMessage should be filtered(ignored/skipped)
func filterCDCMessage(msgType string, recoverStartOffset int64, recoverEndOffset int64, recoverStartTimestampEpochNanoSeconds float64, recoverEndTimestampEpochNanoSeconds float64, currentOffset int64, updatedTimestamp float64) bool {
	// Only Upsert/Delete CDCMessage should be processed. The others should be skipped
	if (msgType != CDCMessageTypeUpsert) && (msgType != CDCMessageTypeDelete) {
		return true
	}

	// if recoverStartOffset is specified, which is not the default 0, it has precedence over recoverStartTimestampEpochNanoSeconds
	// TODO: this code might not needed if the ConsumerPartitions starting from the offset for that partition.
	if recoverStartOffset > KafkaStartOffsetDefault {
		return currentOffset < recoverStartOffset
	}

	// if recoverEndOffset is specified,  then filter the offset greater than it,  only when the Start Offset is also specified.
	if recoverStartOffset > KafkaStartOffsetDefault && recoverEndOffset > KafkaEndOffsetDefault {
		return currentOffset >= recoverEndOffset
	}

	// use recoverStartTimestampEpochNanoSeconds to filter updatedTimestamp
	if recoverStartTimestampEpochNanoSeconds <= updatedTimestamp {
		return false
	}

	// if recoverEndTimestampEpochNanoSeconds is specified (default is -1), return false if it's > updatedTimestamp
	if recoverEndTimestampEpochNanoSeconds > 0.0 && recoverEndTimestampEpochNanoSeconds > updatedTimestamp {
		return false
	}

	return true
}

// doUpsert inserts/updates the multi-rows data in one upsert statement. The data should be de-duplicated already by the key.
func doUpsert(ctx context.Context, conn *pgxpool.Pool, dbName string, tableName string, closing chan struct{}, data map[string]map[string]interface{}) error {
	var columnNamesMap = make(map[string]struct{})
	var columnNamesSortedSlice []string
	//var queryParams []interface{}
	var queryParams []interface{}
	select {
	case <-ctx.Done():
		fmt.Printf("ctx.Done(), exiting doDelete()\n")
		return nil
	case <-closing:
		fmt.Printf("receive closing signal, exiting doDelete()\n")
		return nil
	default:

		// first get all the column names sorted within the batch of all messages.
		// The reason not to just use one record is that the table could be altered with more columns.
		// Also filter out some crdb_internal prefix column(s).
		for _, rowImage := range data {
			for columnName := range rowImage {
				if !strings.HasPrefix(columnName, CDCInternalColumnPrefix) {
					columnNamesMap[columnName] = struct{}{}
				}
			}
		}

		// sort the columnNames so it's consistently matching the column-list vs. values-list
		// if there is newly added column(s),  and some old message does not have the key-value in the kafka message, treat it as nil/NULL.
		for columnName := range columnNamesMap {
			columnNamesSortedSlice = append(columnNamesSortedSlice, columnName)
		}
		sort.Strings(columnNamesSortedSlice)

		// construct the upsert SQL statement. e.g.
		// UPSERT INTO "<dbName>"."<tableName>" (<column1>, <column2>...) VALUES ($1, $2...), ($<n>, $<n+1>...)...
		sqlStmt := fmt.Sprintf("UPSERT INTO \"%s\".\"%s\" (", dbName, tableName)
		for i, columnName := range columnNamesSortedSlice {
			if i == 0 {
				sqlStmt += fmt.Sprintf("\"%s\"", columnName)
				continue
			}
			sqlStmt += fmt.Sprintf(",\"%s\"", columnName)
		}
		sqlStmt += ") VALUES "
		// Construct the bind variables, $1, $2..
		i := 1
		for {
			if i > len(data)*len(columnNamesSortedSlice) {
				// break out of the loop
				break
			}
			if i > 1 {
				sqlStmt += ", "
			}
			for j := 1; j <= len(columnNamesSortedSlice); j++ {
				if j == 1 {
					sqlStmt += fmt.Sprintf("($%d", i)
				} else {
					sqlStmt += fmt.Sprintf(", $%d", i)
				}
				i++
			}
			sqlStmt += ")"
		}
		// populate the queryParams from data
		for _, rowImage := range data {
			for _, columnName := range columnNamesSortedSlice {
				columnValue, ok := rowImage[columnName]
				if !ok {
					// this might be some new columns added. set this to nil/NULL
					columnValue = nil
				}
				queryParams = append(queryParams, columnValue)
			}
		}
		// fmt.Printf("doUpsert() DEBUG, sqlStmt: %v, queryParams: %v\n", sqlStmt, queryParams)
		err := dbExecuteUpsertDelete(ctx, conn, sqlStmt, queryParams)
		if err != nil {
			fmt.Printf("doUpsert() error: %v, sqlstm: %v, queryParams: %v\n", err, sqlStmt, queryParams)
			return errors.Wrapf(err, "doUpsert() error: ")
		}

		return nil
	}
}

// doDelete deletes the multi-rows data in one delete statement using the in-clause. e.g. delete from <table> where (pk_col1, pk_col2...) in ( (<value1>, <value2>...), (<value3>, <value4>...))
func doDelete(ctx context.Context, conn *pgxpool.Pool, dbName string, tableName string, closing chan struct{}, pkColumns []string, data map[string]map[string]interface{}) error {
	//var queryParams []interface{}
	var queryParams []interface{}
	select {
	case <-ctx.Done():
		fmt.Printf("ctx.Done(), exiting doDelete()\n")
		return nil
	case <-closing:
		fmt.Printf("receive closing signal, exiting doDelete()\n")
		return nil
	default:
		sqlStmt := fmt.Sprintf("DELETE FROM \"%s\".\"%s\" WHERE (", dbName, tableName)
		for i, pkColumnName := range pkColumns {
			if i == 0 {
				sqlStmt += fmt.Sprintf("\"%s\"", pkColumnName)
				continue
			}
			sqlStmt += fmt.Sprintf(",\"%s\"", pkColumnName)
		}
		sqlStmt += ") IN ("
		// Construct the bind variables, $1, $2..
		i := 1
		for {
			if i > len(data)*len(pkColumns) {
				// break out of the loop
				break
			}
			if i > 1 {
				sqlStmt += ", "
			}
			for j := 1; j <= len(pkColumns); j++ {
				if j == 1 {
					sqlStmt += fmt.Sprintf("($%d", i)
				} else {
					sqlStmt += fmt.Sprintf(", $%d", i)
				}
				i++
			}
			sqlStmt += ")"
		}
		sqlStmt += ")"
		// populate queryParams from data
		for _, rowImage := range data {
			for _, pkColumnName := range pkColumns {
				pkColumnValue, ok := rowImage[pkColumnName]
				if !ok {
					fmt.Printf("doDelete() pk column name: %v not found in rowImageMap: %v\n", pkColumnName, rowImage)
					return errors.Errorf("doDelete() pk column name: %v not found in rowImageMap: %v", pkColumnName, rowImage)
				}
				//pkColumnValueString, ok := pkColumnValue.(string)
				//if !ok {
				//	fmt.Printf("dbDelete(): error converting columnValue: %v to string\n", pkColumnValue)
				//	return errors.Errorf("dbDelete(): error converting columnValue: %v to string", pkColumnValue)
				//}
				//queryParams = append(queryParams, pkColumnValueString)
				queryParams = append(queryParams, pkColumnValue)
			}
		}
		// fmt.Printf("doDelete() DEBUG, sqlStmt: %s, queryParams: %v\n", sqlStmt, queryParams)
		err := dbExecuteUpsertDelete(ctx, conn, sqlStmt, queryParams)
		if err != nil {
			fmt.Printf("doDelete() error: %v, sqlstmt: %v, queryParams: %v\n", err, sqlStmt, queryParams)
			return errors.Wrapf(err, "doDelete() error: ")
		}

		return nil
	}
}

// convertInterfaceToMap converts the interface{} of the rowImage to the map[string]interface{}
func convertInterfaceToMap(rowImage interface{}) (map[string]interface{}, error) {
	rowImageMap, ok := rowImage.(map[string]interface{})
	if !ok {
		fmt.Printf("rowImage is not a map, %v", rowImage)
		return nil, errors.Errorf("rowImage is not a map, %v", rowImage)
	}
	return rowImageMap, nil
}

// dbExecuteUpsertDelete executes the upsert/delete (and other) SQL statements
func dbExecuteUpsertDelete(ctx context.Context, conn *pgxpool.Pool, sqlStmt string, queryParams []interface{}) error {
	//func dbExecuteUpsertDelete(ctx context.Context, conn *pgxpool.Pool, sqlStmt string, queryParams []interface{}) error {
	var dbErr error
	//queryParamsAny := make([]any, len(queryParams))
	//for i, queryParamAny := range queryParams {
	//	queryParamsAny[i] = queryParamAny
	//}
	for i := 0; i <= DBErrorRetriesDefault; i++ {
		// ignore the pgconn.CommandTag{} for now
		_, dbErr = conn.Exec(ctx, sqlStmt, queryParams...)
		if dbErr == nil {
			return nil
		}
		fmt.Printf("dbExecuteUpsertDelete() error executing query: %v, queryParams: %v, retry#: %d, dbErr: %v\n", sqlStmt, queryParams, i+1, dbErr)
		time.Sleep(100 * time.Millisecond)
	}
	if dbErr != nil {
		fmt.Printf("dbExecuteUpsertDelete() error executing delete query: %v, queryParams: %v, dbErr: %v\n", sqlStmt, queryParams, dbErr)
		return errors.Wrapf(dbErr, "dbExecuteUpsertDelete() error executing query: %v, queryParams: %v", sqlStmt, queryParams)
	}
	return nil
}

// GetDBConnectionPoolBalance returns the database connection pools for each partition, which has 2, one for upsert, one for delete
// The cockroach CLI makes one connection to the DB, which is like the "bootstrap-server" list in Kafka
// Using the bootstrap-server, it can discover all the nodes in the cluster. and optionally filter by locality/region
// The idea is to spread the database connections to the nodes in the cluster to avoid overloading one gateway node.
// The algorithm to distribute the crdb nodes connection to the Kafka partition is round-robin.  Current implementation is sticky during the entire recovery.
// Expose this function from the cli package
// TODO(gli):  make it rebalance in case of  node failure(s)/drain/decommission and/or cluster expansion/shrinking,  or based on the load(cpu)/latency, redistribute/rebalance the database connections.
func GetDBConnectionPoolBalance(ctx context.Context, conn clisqlclient.Conn, topicPartitions []int32, filterLocality string) (map[int32]map[string]*pgxpool.Pool, error) {
	var dbConnPools = make(map[int32]map[string]*pgxpool.Pool)
	var advertiseAddresses []string
	// double-quote database_name for the mix-case scenario
	nodeAdvertiseAddressQuery := fmt.Sprintf("SELECT node_id, advertise_address from crdb_internal.gossip_nodes where is_live=true and locality like '%%%s%%'", filterLocality)
	_, rows, err := sqlExecCtx.RunQuery(
		ctx,
		conn,
		clisqlclient.MakeQuery(nodeAdvertiseAddressQuery),
		false, /* showMoreChars */
	)

	if err != nil {
		return nil, errors.Wrapf(err, "GetDBConnectionPoolBalance(), error executing query: %s", nodeAdvertiseAddressQuery)
	}

	for _, advertiseAddress := range rows {
		advertiseAddresses = append(advertiseAddresses, advertiseAddress[1])
	}
	totalAdvertiseAddresses := len(advertiseAddresses)

	//fmt.Printf("advertiseAddresses: %v, totalAdvertiseAddresses: %v\n", advertiseAddresses, totalAdvertiseAddresses)
	// Use the list of advertise addresses to form the pgURL with all the input from the CLI
	// The below is a shortcut/hack to replace the CLI pgURL <host>:<port> with the advertise_address
	// get URL from the cockroach CLI
	pgOriginalURL := conn.GetURL()
	re := regexp.MustCompile("(postgresql://.*@)(.*:.*?)(/.*)")
	// round-robin (using modulus) assign the node to the partition with upsert/delete.
	for i, partition := range topicPartitions {
		partitionURL := re.ReplaceAllString(pgOriginalURL, fmt.Sprintf("${1}%s${3}", advertiseAddresses[i%totalAdvertiseAddresses]))
		//fmt.Printf("advertiseAddresses[i%%totalAdvertiseAddresses]: %v, partitionURL: %v, pgOriginalURL: %v\n", advertiseAddresses[i%totalAdvertiseAddresses], partitionURL, pgOriginalURL)
		dbConnPoolByType, err := GetDBConnByTypesByURL(ctx, partitionURL)
		if err != nil {
			// clean up existing db connections if any
			cleanupDBConnPools(dbConnPools)
			return nil, errors.Wrapf(err, "GetDBConnectionPoolBalance() error making db connection for partition %v:", partition)
		}
		dbConnPools[partition] = dbConnPoolByType
	}

	return dbConnPools, nil
}

// GetDBConnectionPoolSingleNode returns the database connection pools for each partition, which has 2, one for upsert, one for delete
// Unlike the GetDBConnectionPoolBalance which tries to distribute the load across more gateway nodes in the cluster,
// this function gets the connection to the same host from cockroach CLI.
// There are use cases, where multiple cockroach recoverfromcdc commands are started in parallel on the same hosts or multiple hosts for specific partitions.
// In that case, each process can pin to certain gateway node for the partition list.  The balance is done externally.
func GetDBConnectionPoolSingleNode(ctx context.Context, conn clisqlclient.Conn, topicPartitions []int32) (map[int32]map[string]*pgxpool.Pool, error) {
	var dbConnPools = make(map[int32]map[string]*pgxpool.Pool)
	// get the same URL as the cockroach CLI
	pgURL := conn.GetURL()
	for _, partition := range topicPartitions {
		var dbConnPoolByType = make(map[string]*pgxpool.Pool)
		fmt.Printf("GetDBConnectionPoolSingleNode() partition: %v, pgURL: %v\n", partition, pgURL)
		dbConnPoolByType, err := GetDBConnByTypesByURL(ctx, pgURL)
		if err != nil {
			// clean up existing db connections if any
			cleanupDBConnPools(dbConnPools)
			return nil, errors.Wrapf(err, "GetDBConnectionPoolSingleNode() error making db connection for partition %v:", partition)
		}
		dbConnPools[partition] = dbConnPoolByType
	}
	return dbConnPools, nil
}

// GetDBConnByTypesByURL returns the *pgxpool.Pool from the pgURL for both upsert & delete
func GetDBConnByTypesByURL(ctx context.Context, pgURL string) (map[string]*pgxpool.Pool, error) {
	var dbConnPoolByType = make(map[string]*pgxpool.Pool)
	var dbConnTypes = []string{CDCMessageTypeUpsert, CDCMessageTypeDelete}
	for _, dbConnType := range dbConnTypes {
		dbConn, err := GetDBConnByURL(ctx, pgURL)
		if err != nil {
			return nil, errors.Wrapf(err, "GetDBConnByTypesByURL() error making db connection for pgURL %v:", pgURL)
		}
		dbConnPoolByType[dbConnType] = dbConn
	}
	return dbConnPoolByType, nil
}

// cleanupDBConnPools closes the DB connection pools
func cleanupDBConnPools(dbConnPools map[int32]map[string]*pgxpool.Pool) {
	fmt.Printf("cleaning up dbConnPools: %v\n", dbConnPools)
	for _, dbConnByTypes := range dbConnPools {
		for _, dbConnByType := range dbConnByTypes {
			dbConnByType.Close()
		}
	}
}

// GetDBConnByURL returns the *pgxpool.Pool from the pgURL
func GetDBConnByURL(ctx context.Context, pgURL string) (*pgxpool.Pool, error) {
	var pool *pgxpool.Pool
	var dbConnErr error
	sleep := DBConnectionMinRetrySleepSeconds // 1 second
	// Try DBErrorRetriesDefault, if still not successful, let the caller decide what to do
	for i := 1; i < DBErrorRetriesDefault; i++ {
		fmt.Printf("GetDBConnByURL() pgURL: %v\n", pgURL)
		poolConfig, err := pgxpool.ParseConfig(pgURL)
		dbConnErr = err
		if err != nil {
			fmt.Printf("Unable to connect to the db. Retrying in %d seconds, err: %v\n", sleep, err)
			time.Sleep(time.Duration(sleep * int(time.Second)))
		} else {
			pool, err = pgxpool.NewWithConfig(ctx, poolConfig)
			dbConnErr = err
			if err != nil {
				fmt.Printf("Unable to connect to the db. Retrying in %d seconds, err: %v\n", sleep, err)
				time.Sleep(time.Duration(sleep * int(time.Second)))
			} else {
				break
			}
		}
		if sleep < DBConnectionMaxRetrySleepSeconds {
			sleep += DBConnectionMinRetrySleepSeconds
		}
	}

	return pool, dbConnErr
}

// GetDBConnectionPool returns the database connection pools. if balance is not specified, use the node from the CLI
// otherwise, round-robin assign the live nodes in the cluster to the kafka topic partition.
func GetDBConnectionPool(ctx context.Context, conn clisqlclient.Conn, topicPartitions []int32, useBalanceConnection bool, filterLocality string) (map[int32]map[string]*pgxpool.Pool, error) {
	var dbConnPools = make(map[int32]map[string]*pgxpool.Pool)
	var dbErr error
	if useBalanceConnection {
		dbConnPools, dbErr = GetDBConnectionPoolBalance(ctx, conn, topicPartitions, filterLocality)
		if dbErr != nil {
			return nil, errors.Wrapf(dbErr, "Failed to get dbConnPools (Balance) for topicPartitions: %v\n", topicPartitions)
		}
		return dbConnPools, nil
	}
	dbConnPools, dbErr = GetDBConnectionPoolSingleNode(ctx, conn, topicPartitions)
	if dbErr != nil {
		return nil, errors.Wrapf(dbErr, "Failed to get dbConnPools (SingleNode) for topicPartitions: %v\n", topicPartitions)
	}
	return dbConnPools, nil
}

// NewSaramaConfig returns the sarama config, the clientType is either Consumer or Producer.
func NewSaramaConfig(kafkaConsumerProducerConfigFile string, kafkaBootstrapServer string, kafkaTopicName string, clientType string) (*sarama.Config, string, string, error) {
	outKafkaBootstrapServer := kafkaBootstrapServer
	outKafkaTopicName := kafkaTopicName
	saramaConfig := sarama.NewConfig()
	if len(kafkaConsumerProducerConfigFile) > 0 {
		if _, err := os.Stat(kafkaConsumerProducerConfigFile); errors.Is(err, os.ErrNotExist) {
			return nil, kafkaBootstrapServer, kafkaTopicName, errors.Errorf("RecoverKafkaCommandConfigFile: %v does not exist.", kafkaConsumerProducerConfigFile)
		}
		// Read the properties file
		props := properties.MustLoadFile(kafkaConsumerProducerConfigFile, properties.UTF8).Map()
		// fmt.Printf("props: %v\n", props)
		// check if Kafka Auth is provided.
		// TODO: move this to another package/file/function.  This only handles a subset of cases. complete logic is in "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/sink_kafka.go"
		if saslMechanism, ok := props[KafkaParamSASLMechanism]; ok {
			saramaConfig.Net.SASL.Enable = true
			saramaConfig.Net.SASL.Handshake = true
			saramaConfig.Net.SASL.Mechanism = sarama.SASLMechanism(saslMechanism)
			// switch saramaConfig.Net.SASL.Mechanism
			// TODO: handle sarama.SASLTypeSCRAMSHA512, sarama.SASLTypeSCRAMSHA256, sarama.SASLTypeOAuth

			if securityProtocol, ok := props[KafkaParamSecurityProtocol]; ok && securityProtocol == "SASL_SSL" {
				saramaConfig.Net.TLS.Enable = true
				// TODO(gli): make this configurable in the property file.  Auth is provided, always set tlsSkipVerify to true for now.
				saramaConfig.Net.TLS.Config = &tls.Config{
					InsecureSkipVerify: true,
				}
			}

			if saslJaasConfig, ok := props[KafkaParamSASLJAASConfig]; ok {
				// fmt.Printf("saslJaasConfig: %v\n", saslJaasConfig)
				re := regexp.MustCompile(`username=\s*"([^"]*)"\s+password=\s*"\s*([^"]*)"`)
				credential := re.FindAllStringSubmatch(saslJaasConfig, -1)
				// fmt.Printf("credential[0][1]: %v, credential[0][2]:%v\n", credential[0][1], credential[0][2])
				saramaConfig.Net.SASL.User = credential[0][1]
				saramaConfig.Net.SASL.Password = credential[0][2]
			} else {
				return nil, kafkaBootstrapServer, kafkaTopicName, errors.Errorf("RecoverKafkaCommandConfigFile: %v missing sasl.jaas.config", kafkaConsumerProducerConfigFile)
			}

			// Only handle the ssl.truststore.type=PEM for now
			if sslTrustStoreType, ok := props[KafkaParamSSLTrustStoreType]; ok && sslTrustStoreType == "PEM" {
				if sslTrustStoreLocation, ok := props[KafkaParamSSLTrustStoreLocation]; ok && len(sslTrustStoreLocation) > 0 {
					fileContent, err := os.ReadFile(sslTrustStoreLocation)
					if err != nil {
						return nil, kafkaBootstrapServer, kafkaTopicName, errors.Wrapf(err, "error reading from ssl.truststore.location: %v.", sslTrustStoreLocation)
					}
					caCertPool := x509.NewCertPool()
					caCertPool.AppendCertsFromPEM(fileContent)
					saramaConfig.Net.TLS.Config.RootCAs = caCertPool
				}
			}

		}

		// clientID in the command-config consumer/producer properties file override the default
		saramaConfig.ClientID = fmt.Sprintf("CockroachDBRecoverFromCDC-%v-%v", clientType, kafkaTopicName)
		if clientID, ok := props[KafkaParamClientID]; ok && len(clientID) > 0 {
			saramaConfig.ClientID = clientID
		}

		if len(outKafkaBootstrapServer) == 0 {
			if bootstrapServers, ok := props[KafkaParamBootstrapServers]; ok && len(bootstrapServers) > 0 {
				outKafkaBootstrapServer = bootstrapServers
			} else {
				return nil, kafkaBootstrapServer, kafkaTopicName, errors.Errorf("bootstrap.servers needs to be specified in the property file or CLI.")
			}
		}

		if len(outKafkaTopicName) == 0 {
			if topicName, ok := props[KafkaParamTopicName]; ok && len(topicName) > 0 {
				outKafkaTopicName = topicName
			} else {
				return nil, kafkaBootstrapServer, kafkaTopicName, errors.Errorf("topic.name needs to be specified in the property file or CLI.")
			}
		}

		// TODO: parse other consumer/producer configs, and set in the Sarama config.
	}

	// TODO: AutoCommit needs to be set to false to prevent data loss in case the upsert/delete fails.
	if clientType == KafkaClientTypeConsumer {
		saramaConfig.Consumer.Offsets.AutoCommit.Enable = true
		//saramaConfig.Consumer.Fetch.Min = 1
		//saramaConfig.Consumer.Fetch.Default = ConsumerMaxMessageBytes
		// consume from the oldest.  unless the offset is specified in CLI to resume from previous interrupted recovery
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
		saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategySticky}
		saramaConfig.Consumer.Return.Errors = false
		// increase when seeing "abandoned subscription to...because consuming was taking too long" or check slow writing to destination
		saramaConfig.Consumer.MaxProcessingTime = 30000 * time.Millisecond
	}

	if clientType == KafkaClientTypeProducer {
		// WaitForLocal RequiredAcks = 1 should be good enough with much higher throughput, unless there are broker failures in the kafka cluster.
		// TODO(gli): get this setting from the producer property file. AtLeastOnce delivery:  WaitForAll RequiredAcks = -1, The fire-and-forget should not be used:  NoResponse RequiredAcks = 0
		saramaConfig.Producer.RequiredAcks = sarama.WaitForLocal
		saramaConfig.Producer.Retry.Max = 100000
		saramaConfig.Producer.Partitioner = sarama.NewHashPartitioner
		// The SyncProducer requires this is set to true.  if it's AsyncProducer, set this to false.
		saramaConfig.Producer.Return.Successes = true
		saramaConfig.Producer.Flush.Bytes = KafkaConsumerMaxMessageBytes
		saramaConfig.Producer.Flush.Messages = 10000
		saramaConfig.Producer.Flush.Frequency = 1000 * time.Millisecond
	}

	// Increase Retry in case of client/metadata got error from broker <broker.id> while fetching metadata: EOF
	saramaConfig.Metadata.Retry.Max = 100000
	// The sarama.V2_1_0_0 can support zstd compression
	saramaConfig.Version = sarama.V2_1_0_0

	return saramaConfig, outKafkaBootstrapServer, outKafkaTopicName, nil
}
