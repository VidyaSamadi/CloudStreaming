
#This code generates a log data that connects  and sends the logs to Pub. Users need to create a PublisherClient object, and then call the publish function
# For query regarding the Big data used herein please contact Vidya Samadi at Clemson University (samadi@clemson.edu)


from stream_logs import generate_log_line
import logging
import random
import time


PROJECT_ID="VidyaSamadiData"
TOPIC = "Vidyalogs"


publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC)



def publish(publisher, topic, message):
    data = message.encode('utf-8')
    return publisher.publish(topic_path, data = data)



def callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(
            topic_name, message_future.exception()))
    else:
        print(message_future.result())


if __name__ == '__main__':

    while True:
        line = generate_log_line()
        print(line)
        message_future = publish(publisher, topic_path, line)
        message_future.add_done_callback(callback)

        sleep_time = random.choice(range(1, 3, 1))
        time.sleep(sleep_time)
