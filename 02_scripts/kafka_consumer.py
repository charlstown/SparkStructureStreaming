from kafka import KafkaConsumer
import sys, os

if __name__ == "__main__":
    try:
        print("Initialization...")
        # consumer = KafkaConsumer(bootstrap_servers='172.20.1.21:9092',
        #                          auto_offset_reset='earliest')
        consumer = KafkaConsumer(bootstrap_servers='127.0.0.1:9092',
                                 auto_offset_reset='earliest')
        consumer.subscribe(['metrics'])

        for message in consumer:
            print (message.value)

        print("End")

    except KeyboardInterrupt:
        print('Interrupted from keyboard, shutdown')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

