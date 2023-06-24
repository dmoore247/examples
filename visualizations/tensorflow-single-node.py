# Databricks notebook source
# MAGIC %md ## TensorFlow tutorial - MNIST For ML Beginners
# MAGIC
# MAGIC This notebook demonstrates how to use TensorFlow on the Spark driver node to fit a neural network on MNIST handwritten digit recognition data.
# MAGIC
# MAGIC Prerequisites:
# MAGIC * A GPU-enabled cluster on Databricks.
# MAGIC * TensorFlow installed with GPU support.
# MAGIC
# MAGIC The content of this notebook is [copied from TensorFlow project](https://www.tensorflow.org/versions/r0.11/tutorials/index.html) under [Apache 2.0 license](https://github.com/tensorflow/tensorflow/blob/master/LICENSE) with slight modification to run on Databricks. Thanks to the developers of TensorFlow for this example!

# COMMAND ----------

import tensorflow as tf

# COMMAND ----------

# Some of this code is licensed by Google under the Apache 2.0 License

# Copyright 2015 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

# COMMAND ----------

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

# Import data
from tensorflow.examples.tutorials.mnist import input_data

import tensorflow as tf

# COMMAND ----------

# MAGIC %md Load the data (this step may take a while)

# COMMAND ----------

mnist = input_data.read_data_sets('/tmp/data', one_hot=True)

# COMMAND ----------

# MAGIC %md Define the model

# COMMAND ----------

x = tf.placeholder(tf.float32, [None, 784])
W = tf.Variable(tf.zeros([784, 10]))
b = tf.Variable(tf.zeros([10]))
y = tf.matmul(x, W) + b

# COMMAND ----------

# MAGIC %md Define loss and optimizer

# COMMAND ----------

y_ = tf.placeholder(tf.float32, [None, 10])

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC The raw formulation of cross-entropy,
# MAGIC
# MAGIC ```tf.reduce_mean(-tf.reduce_sum(y_ * tf.log(tf.softmax(y)), reduction_indices=[1]))```
# MAGIC
# MAGIC can be numerically unstable.
# MAGIC
# MAGIC So here we use `tf.nn.softmax_cross_entropy_with_logits` on the raw
# MAGIC outputs of 'y', and then average across the batch.

# COMMAND ----------

cross_entropy = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits=y, labels=y_))
train_step = tf.train.GradientDescentOptimizer(0.5).minimize(cross_entropy)

# COMMAND ----------

correct_prediction = tf.equal(tf.argmax(y, 1), tf.argmax(y_, 1))
accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))
summary = tf.summary.scalar("accuracy", accuracy)

# COMMAND ----------

# MAGIC %md
# MAGIC Start TensorBoard so you can monitor training progress.

# COMMAND ----------

log_dir = "/tmp/tensorflow_log_dir"
dbutils.tensorboard.start(log_dir)

# COMMAND ----------

# MAGIC %md Train model using small batches of data.

# COMMAND ----------

sess = tf.InteractiveSession()

# Make sure to use the same log directory for both start TensorBoard in your training.
summary_writer = tf.summary.FileWriter(log_dir, graph=sess.graph)

tf.global_variables_initializer().run()
for batch in range(1000):
  batch_xs, batch_ys = mnist.train.next_batch(100)
  _, batch_summary = sess.run([train_step, summary], feed_dict={x: batch_xs, y_: batch_ys})
  summary_writer.add_summary(batch_summary, batch)

# COMMAND ----------

# MAGIC %md Test the trained model. The final accuracy is reported at the bottom. You can compare it with the accuracy reported by the other frameworks!

# COMMAND ----------

print(sess.run(accuracy, feed_dict={x: mnist.test.images,
                                    y_: mnist.test.labels}))

# COMMAND ----------

# MAGIC %md
# MAGIC TensorBoard stays active after your training is finished so you can view a summary of the process, even if you detach your notebook. Use `dbutils.tensorboard.stop()` from this or any notebook to stop TensorBoard.

# COMMAND ----------

dbutils.tensorboard.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC If you place your log directory in `/tmp/` it will be deleted when your cluster shutsdown. If you'd like to save your training logs you can copy them to a permanent location, for example somewhere on the Databricks file system.

# COMMAND ----------

import os
import shutil
log_dir_dbfs = "/dbfs/tensorflow/logs"

if os.path.exists(log_dir_dbfs):
  shutil.rmtree(log_dir_dbfs)
shutil.move(log_dir, log_dir_dbfs)

# COMMAND ----------

_ga=GA1.2.914701913.1578670647; SSESS505aadec12321d72d448e7366c49ab96=5l9ccnl58oskd3h5cqi6om2k67;
_vwo_uuid_v2=D27BDC0C78B5B79A446456DF9C9A454C7|3d71a7b285be3d63ba7e4f2718a2ada7; 
_fbp=fb.1.1578807448134.1403460338; _mkto_trk=id:094-YMS-629&
  token:_mch-databricks.com-1578807448161-75983; 
    _vis_opt_s=1%7C; _vis_opt_test_cookie=1; _hjid=98e736f8-bcb0-4ca4-a054-6460d77f667b; jupyterhub-session-id=2212e44b033c48b88d926d4c4fac1b26; _xsrf=2|8aa9afa1|1f44a81d9feea8db02dcffe39250d189|1579206815; oribi_user_guid=a60e6f0c-bd50-d09f-24ea-e0cab9860afd; jupyterhub-user-douglas.moore%40databricks.com=2|1:0|10:1579224978|46:jupyterhub-user-douglas.moore%40databricks.com|40:SGNRNUVyZ1dzM3E1dm9zelhKWUtweGR6dnNUMkdB|d342bd1c38f8a71764a0ca0286167728c0450c1ed8694f8114053b3ff5987c8f; workspace-url=atp.cloud.databricks.com|demo.cloud.databricks.com|logfood.cloud.databricks.com; _hp2_id.1473692602=%7B%22userId%22%3A%226579013795465578%22%2C%22pageviewId%22%3A%222646137081599257%22%2C%22sessionId%22%3A%225140533090861543%22%2C%22identity%22%3A%2298350ba4b275bf552be460539437ada0ba4d57db1a814eadadd2133dafb86a9f%22%2C%22trackerVersion%22%3A%224.0%22%2C%22identityField%22%3A%22hashedId%22%2C%22isIdentified%22%3A1%2C%22oldIdentity%22%3Anull%7D; session=eyJjc3JmX3Rva2VuIjoiMGQ0OGM1OTAzNDc3YjgyOWM4MjBlMzc2YTRhMzQ3MDZjZmFlZDFjZCJ9.Xihd9g.8R7y2_ljm29vsAyc3BLfT3jhgJI; _gid=GA1.2.586536959.1580076749; _hp2_id.3428506230=%7B%22userId%22%3A%225235307410734782%22%2C%22pageviewId%22%3A%222167360852001397%22%2C%22sessionId%22%3A%226594551459192768%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D; JSESSIONID=webapp-webapp-78fffd7bfd-kzwc81suco3lyvza9p1dlth70d57qoj.webapp-webapp-78fffd7bfd-kzwc8
