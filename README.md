# Autonomous Bicycle Collision Detection or A.B.C.D

#### Why Another Data Pipeline? 

While studying for the GCP Professional Data Engineer Cert, I wanted to try out Dataflow/Apache Beam. What better way to do than take open source data and see how fast you can stream it into a data warehouse? 

I decided to go with Citibikes' data stream to collect information on how many bikes are docked at each station in NYC. Through this information, I can calculate how many bikes are roaming around the citi. 

That's what this repository focuses on. There are other parts to this project as well, such as, checking vehicle congestion across the city and storing that information in our fancy data warehouse, i.e. BigQuery. By combing these two pieces, we can try to predict when a collision would happen and where. The last missing piece to this puzzle is historical accident information, which is publicly available on NYC Department of Transportations' Website. 



## Getting Started

This repository contains two key pieces of running the ingestion pipeline:

* Apache Beam code written in Java
* Stream publisher written in Python 3.7.x

And of course, you need to have a GCP account to run the whole pipeline. 

Big shoutout to <a href="https://linuxacademy.com/"> Linux Academy</a> for providing free sandbox accounts in all three major clouds. Yes, I used the GCP sandbox account from Linux academy, and also yes, I passed the cert! 

### Prerequisites

The prerequisites are split into three parts:

##### GCP 

There is a cicd folder that performs:

* login into gcp account
* creates IAM service account called ```runner``` 
* binds permissions fo running the dataflow pipeline
  * ```roles/dataflow.worker```
  * ```roles/pubsub.subscriber```
  * ```roles/bigquery.user```
* and enables Bigquery, Dataflow and PubSub APIs ( I have this in here since the Linux Academy sandbox expires every few hours)

##### Dataflow 

Dataflow pipeline requires Java and maven installed:

I used <a href="https://github.com/asdf-vm/asdf">asdf</a> for runtime version management and used the following versions:

```bash
$ asdf plugin-add maven
$ asdf install maven 3.5.4
$ asdf plugin-add java 
$ asdf install java adopt-openjdk-8u242-b08_openj9-0.18.1
```

##### Stream Publisher

This one is a Python script, so we make our virtualenv and install dependencies. I prefer using <a href="https://pypi.org/project/pipenv/"> pipenv </a> but with my own virtualenv. 

```bash
$ asdf local python 3.7.5 # set python version to 3.7.5
$ python -V 
Python 3.7.5
$ python -m ven venv
$ source venv/bin/activate
$ pip install -U pip setuptools
$ pip install pipenv
$ pipenv install
```



### Running the Pipeline

For Dataflow Pipeline

```bash
$ cd ~/abcd/pipeline
$ mvn compile exec:java -Dexec.mainClass=com.abcd.citibike.AverageBikes \
     -Dexec.args="--runner=DataflowRunner --project=$DEVSHELL_PROJECT_ID \
                  --topic=<pub-sub-topic-name> \
                  --destination=gs://<your-gcs-bucket>/ \
                  --bikeTable=<bq-dataset>.<table> \
                  " 
```

And for Publisher

```bash
$ cd ~/abcd/pub_sub
$ python publish.py
```

Here's the beautiful Dataflow pipeline running:

![Pipeline](https://github.com/aarora08/ABCD/blob/master/static/2020-05-23%2012.13.30.jpg)

## Contributing

All suggestions are welcome, use a PR and as the sole maintainer of this tiny project, I'll see what I can review and approve!

## Authors

* **Arshit Arora**

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

