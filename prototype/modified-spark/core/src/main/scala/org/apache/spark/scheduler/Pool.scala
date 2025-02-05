/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off

package org.apache.spark.scheduler

import java.io.OutputStreamWriter
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.matching.Regex

import org.json4s.jackson.Serialization.write
import org.json4s.{DefaultFormats, Formats}

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * A Schedulable entity that represents collection of Pools or TaskSetManagers
 */
private[spark] class Pool(
    val poolName: String,
    val schedulingMode: SchedulingMode,
    initMinShare: Int,
    initWeight: Int)
  extends Schedulable with Logging {

  val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]
  val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]
  val weight = initWeight
  val minShare = initMinShare
  var runningTasks = 0
  val priority = 0

  // A pool's stage id is used to break the tie in scheduling.
  var stageId = -1
  val name = poolName
  var parent: Pool = null

  // New variable to store the result from the external API
  var externalApiQueue: ConcurrentLinkedQueue[Schedulable] = new ConcurrentLinkedQueue[Schedulable]()

  private val taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()
      case _ =>
        val msg = s"Unsupported scheduling mode: $schedulingMode. Use FAIR or FIFO instead."
        throw new IllegalArgumentException(msg)
    }
  }

  override def isSchedulable: Boolean = true


  override def addSchedulable(schedulable: Schedulable): Unit = {
    require(schedulable != null)
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    schedulable.parent = this
    // call external API to refresh the externalApiQueue
    externalApiQueue = getTaskFromApi(schedulableQueue)
  }

  override def removeSchedulable(schedulable: Schedulable): Unit = {
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)
    // call external API to refresh the externalApiQueue
    externalApiQueue = getTaskFromApi(schedulableQueue)
  }

  private def getTaskFromApi(queue: ConcurrentLinkedQueue[Schedulable]): ConcurrentLinkedQueue[Schedulable] = {
    implicit val formats: Formats = DefaultFormats

    val url = new URL("http://192.168.1.10:14040/task")
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("POST")
    connection.setRequestProperty("Content-Type", "application/json")
    connection.setDoOutput(true)

    val jsonQueue = write(queue.asScala.toList)
    val outputStream = new OutputStreamWriter(connection.getOutputStream)
    outputStream.write(jsonQueue)
    outputStream.flush()
    outputStream.close()

    val response = Source.fromInputStream(connection.getInputStream).mkString
    connection.disconnect()

    // Check if the response is "[]"
    if (response.trim == "[]") {
      return queue
    }

    // Extract the order of stageIds using regular expressions
    val stageIdPattern: Regex = """"stageId":\s*(\d+)""".r
    val manipulatedOrder = stageIdPattern.findAllMatchIn(response).map(_.group(1).toInt).toList

    // Reorder the original list based on the manipulated order
    val originalList = queue.asScala.toList
    val reorderedList = manipulatedOrder.flatMap { stageId =>
      originalList.find(_.stageId == stageId)
    }

    // Create a new ConcurrentLinkedQueue and add the reordered Schedulable objects
    val manipulatedQueue = new ConcurrentLinkedQueue[Schedulable]()
    reorderedList.foreach(manipulatedQueue.add)

    //// Check if the first and last tasks are swapped
    // I no longer have to do this since I have verified that I can extract the order from the external API
    // val manipulatedQueueList = manipulatedQueue.asScala.toList
    // if (originalList.size >= 2) {
    //   val originalFirst = originalList.head
    //   val originalLast = originalList.last
    //   val manipulatedFirst = manipulatedQueueList.head
    //   val manipulatedLast = manipulatedQueueList.last

    //   if (originalFirst != manipulatedLast || originalLast != manipulatedFirst) {
    //     throw new IllegalStateException("The first and last tasks in the queue were not swapped correctly by the external API.")
    //   }
    // }

    manipulatedQueue
  }

  override def getSchedulableByName(schedulableName: String): Schedulable = {
    if (schedulableNameToSchedulable.containsKey(schedulableName)) {
      return schedulableNameToSchedulable.get(schedulableName)
    }
    for (schedulable <- schedulableQueue.asScala) {
      val sched = schedulable.getSchedulableByName(schedulableName)
      if (sched != null) {
        return sched
      }
    }
    null
  }

  override def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit = {
    schedulableQueue.asScala.foreach(_.executorLost(executorId, host, reason))
  }

  override def executorDecommission(executorId: String): Unit = {
    schedulableQueue.asScala.foreach(_.executorDecommission(executorId))
  }

  override def checkSpeculatableTasks(minTimeToSpeculation: Long): Boolean = {
    var shouldRevive = false
    for (schedulable <- schedulableQueue.asScala) {
      shouldRevive |= schedulable.checkSpeculatableTasks(minTimeToSpeculation)
    }
    shouldRevive
  }

  // override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
  //   val sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
  //   val sortedSchedulableQueue =
  //     schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
  //   for (schedulable <- sortedSchedulableQueue) {
  //     sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue.filter(_.isSchedulable)
  //   }
  //   sortedTaskSetQueue
  // }

  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    val sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    // Convert externalApiQueue to a sequence 
    val sortedSchedulableQueue = externalApiQueue.asScala.toSeq
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue.filter(_.isSchedulable)
    }
    sortedTaskSetQueue
  }

  def increaseRunningTasks(taskNum: Int): Unit = {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }

  def decreaseRunningTasks(taskNum: Int): Unit = {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }
}

// scalastyle:on