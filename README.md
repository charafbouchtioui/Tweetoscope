# Tweetoscope

## Project Description

The tweetoscope project aims to implement a scalable and maintainable application for predicting tweets popularity in real time. The application is divided into a set of microservices communicating via Kafka middleware and deployable on kubernetes.

- [Generator](/generator): The generator is in charge of injecting tweets and retweets into Kafka streams.
- [Collector](/collector): Collects tweets and retweets and sends partial cascades (a cascade is a series of a tweet and its retweets) according to a set of observation windows. It also manages cascade termination to send the true length of the cascade (Tweet popularity).
- [Estimator](/estimator): Uses Marked Hawkes process with exponential kernels to estimate the tweet popularity based on the history of retweets. 
- [Predictor](/predictor): The predictor receives random forest models trained offline on Hawkes parameters to adjust predictions. It also receives the real length of a cascade when it considered terminated, and sends statistics (ARE) of the predictions alongside with samples to train the RF models.
- [Learner](/learner): Retrieves samples sent by the predictor from Kafka streams, when a mini batch of samples for a specific observation window is received, it triggers the training of the models (each observation window is associated with a model). When a batch of data is received, it triggers the hypertunning of the random forests. The trained models are sent to the predictor to improve the predictions.
- [Monitor](/monitor): Acts as a [Prometheus client](https://prometheus.io/docs/instrumenting/clientlibs/) client, retrieves the ARE metric from Kafka streams and sent it the a Prometheus push gateway.
- [Dashboard](/dashboard): Acts as a [Prometheus client](https://prometheus.io/docs/instrumenting/clientlibs/), retrieves predictions and sends alerts to a Prometheus push gateway.


** The popularity ** is received and displayed by the Prometheus Alert Manager which could be configured to send emails or notifications in case of predicting that a tweet would be very popular.

The ARE is also received by Prometheus server, which could be plugged in as a Data Source for Grafana to keep monitoring the accuracy of the predictions.

## Deployment

### Prerequisites

To test the project on your local machine, you can do it using [Minikube](https://minikube.sigs.k8s.io/docs/start/) (Minikube runs a single-node Kubernetes cluster inside a Virtual Machine (VM) on your laptop).

Start the cluster 

> minikube start

You will need [kubectl](https://kubernetes.io/fr/docs/tasks/tools/install-kubectl/) to interact with the minikube cluster.

### You are ready now 

To deploy the application in the started minikube cluster you will need to copy the two files in the [deployment](/deployment) directory, you can clone the project by running 

> git clone https://gitlab-student.centralesupelec.fr/Anas.Laaroussi/tweetoscope.git

You have to start by deploying the middleware

> kubectl apply -f middleware-deployement.yml

This will run the zookeper server, the Kafka broker, the Prometheus server, the Prometheus alert manager and the Prometheus push gateway.

You can deploy the services now 

> kubectl apply -f services-deployement.yml

To see the status of the Kubernetes pods deployed on your minikube cluster, you can run the command

> kubectl get pods 

Once the status of all pods changes to run, you can visualize the performance metric and the alerts about popular tweets.

By default the topics created in the Kafka broker contain 4 partitions, so scaling up an application node to more than 4 replicas is irrelevant. 

In the deployment files only the estimator is launched with 4 replicas, but also the predictor and the learner, are designed in a scalable way.

### View alerts and monitor the application performance

The Prometheus server and the alert manager are exposed via Nodeports. The port for Prometheus is ***30187*** and for the Alertmanager is ***30189***. 

you can access Prometheus from your navigator `<minikube-ip>:30187` 

the command `minikube ip` gives you the ip of your minikube cluster.

to view the alerts go to `<minikube-ip>:30187/alert` or to the alertmanager `<minikube-ip>:30189` .

The ARE can be seen on Prometheus `<minikube-ip>:30187` by typing ARE_600 (for the observation window 600) or ARE_1200 (observation window 1200) in the metrics field. But a better way to monitor the error is to use [Grafana](https://grafana.com/).

You can launch Grafana easily by using a docker image

`docker run -d -p 3000:3000 grafana/grafana`

This will run Grafana in your localhost:3000 .

We supply a dashboard.json in the directory [Grafana](/grafana) to upload the dashboard for visualization. Upload steps can be found in the following [link](https://grafana.com/docs/grafana/latest/dashboards/export-import/).

[Add a prometheus data source](https://prometheus.io/docs/visualization/grafana/) to your Grafana and specify `<minikube-ip>:30187` in your url. The dashboard should show the time evolution of ARE_600 and ARE_1200.

## Documentation

Doxygen [Documentation](https://anas.laaroussi.pages-student.centralesupelec.fr/tweetoscope) was generated for all application nodes except the monitor and the dashboard.


## Integration

### Docker Integration

To use a unique service from the application, docker images can be pulled from this [repository](https://hub.docker.com/repository/docker/anaslaaroussi/tweetoscope)

### Cmake

Install [cppkafka](https://github.com/mfontanini/cppkafka) and [gaml](https://github.com/HerveFrezza-Buet/gaml).

The collector can be compiled and installed as a shared library.
To install the project you have to clone the repository in your machine and in the root directory of the project run


`mkdir build`

`cd build`

`cmake .. -DCMAKE_INSTALL_PREFIX=/usr`

`make -j`

`sudo make install`

`cd ../..`


## Licence

GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.












