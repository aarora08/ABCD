from google.cloud import pubsub_v1
import os


def main():
    subscriber = pubsub_v1.SubscriberClient()

    def callback(message):
        print("Received message: {}".format(message))
        message.ack()

    topic_name = "projects/{project_id}/topics/{topic}".format(
        project_id=os.getenv("DEVSHELL_PROJECT_ID"),
        topic="stations_ingestion",  # Set this to something appropriate.
    )
    subscription_name = "projects/{project_id}/subscriptions/{topic}".format(
        project_id=os.getenv("DEVSHELL_PROJECT_ID"),
        topic="stations_ingestion",  # Set this to something appropriate.
    )
    # subscriber.create_subscription(
    #     name=subscription_name, topic=topic_name)
    future = subscriber.subscribe(subscription_name, callback)
    print(future.result())


if __name__ == "__main__":
    main()
