package org.apache.spark.sql.execution.joins.dns

import java.util.{HashMap => JavaHashMap, ArrayList => JavaArrayList}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, Projection, Expression}
import org.apache.spark.sql.execution.SparkPlan
import scala.collection.mutable.Queue
import java.util.concurrent.ConcurrentHashMap

/**
 * In this join, we are going to implement an algorithm similar to symmetric hash join.
 * However, instead of being provided with two input relations, we are instead going to
 * be using a single dataset and obtaining the other data remotely -- in this case by
 * asynchronous HTTP requests.
 *
 * The dataset that we are going to focus on reverse DNS, latitude-longitude lookups.
 * That is, given an IP address, we are going to try to obtain the geographical
 * location of that IP address. For this end, we are going to use a service called
 * telize.com, the owner of which has graciously allowed us to bang on his system.
 *
 * For that end, we have provided a simple library that makes asynchronously makes
 * requests to telize.com and handles the responses for you. You should read the
 * documentation and method signatures in DNSLookup.scala closely before jumping into
 * implementing this.
 *
 * The algorithm will work as follows:
 * We are going to be a bounded request buffer -- that is, we can only have a certain number
 * of unanswered requests at a certain time. When we initialize our join algorithm, we
 * start out by filling up our request buffer. On a call to next(), you should take all
 * the responses we have received so far and materialize the results of the join with those
 * responses and return those responses, until you run out of them. You then materialize
 * the next batch of joined responses until there are no more input tuples, there are no
 * outstanding requests, and there are no remaining materialized rows.
 *
 */
trait DNSJoin {
  self: SparkPlan =>

  val leftKeys: Seq[Expression]
  val left: SparkPlan

  override def output = left.output

  @transient protected lazy val leftKeyGenerator: Projection =
    newProjection(leftKeys, left.output)

  // How many outstanding requests we can have at once.
  val requestBufferSize: Int = 300

  /**
   * The main logic for DNS join. You do not need to implement anything outside of this method.
   * This method takes in an input iterator of IP addresses and returns a joined row with the location
   * data for each IP address.
   *
   * If you find the method definitions provided to be counter-intuitive or constraining, feel free to change them.
   * However, note that if you do:
   *  1. we will have a harder time helping you debug your code.
   *  2. Iterators must implement next and hasNext. If you do not implement those two methods, your code will not compile.
   *
   * **NOTE**: You should return JoinedRows, which take two input rows and returns the concatenation of them.
   * e.g., `new JoinedRow(row1, row2)`
   *
   * @param input the input iterator
   * @return the result of the join
   */
  def hashJoin(input: Iterator[Row]): Iterator[Row] = {
    new Iterator[Row] {
      // IMPLEMENT ME
      var toOutput = new Queue[Row]()
      var responseCache = new ConcurrentHashMap[Int, Row]()
      var requestBuffer = new ConcurrentHashMap[Int, Row]()
      var requests = 0
      var lastSize = 0
      var first = true

      /**
       * This method returns the next joined tuple.
       *
       * *** THIS MUST BE IMPLEMENTED FOR THE ITERATOR TRAIT ***
       */
      override def next() = {
        //while (toOutput.size == 0) {}
        toOutput.dequeue
      }

      /**
       * This method returns whether or not this iterator has any data left to return.
       *
       * *** THIS MUST BE IMPLEMENTED FOR THE ITERATOR TRAIT ***
       */
      override def hasNext() = {
        if (requestBuffer.size() < 1 && !input.hasNext) {
          false
        } else if (toOutput.nonEmpty) {
          true
        } else {
          makeRequest()
          while (responseCache.size == lastSize && requestBuffer.size != 0) {}                        //busy wait for lookup to return
          val responses = responseCache.keySet.iterator()           //make iterators out of both hashmaps
          lastSize = 0
          while (responses.hasNext) {                                //if there are resonses in the cache
            val responseKey = responses.next()                       //take the next one (its a hashcode of the IP address)
            lastSize += 1
            //var responseKeyIP = leftKeyGenerator(responseKey).getInt(0)  // and get its String
            val requestKeys = requestBuffer.keySet.iterator()        //make iterators out of both hashmaps

            while (requestKeys.hasNext) {                            //if there are uncompleted responses
              val requestKey = requestKeys.next()                    //get the hashcode (by IP address again)
              //var requestIP = leftKeyGenerator(requestKey).getInt(0) //

              if (requestKey == responseKey) {                  // If the hash codes are the same, then the IP's are the same, so we can join.
                var outputNumb = requestBuffer.get(requestKey).getInt(1) //how many times we need to join.
                while (outputNumb > 0) {
                  toOutput.enqueue(new JoinedRow(Row(requestBuffer.get(requestKey).getString(0)), responseCache.get(responseKey)))
                  outputNumb -= 1
                  requests -= 1
                }
                requestBuffer.remove(requestKey)                     //remove from request buffer
              }
            }
          }
          makeRequest()
        }
        toOutput.nonEmpty
      }


      /**
       * This method takes the next element in the input iterator and makes an asynchronous request for it.
       */
      private def makeRequest() = {
        while (input.hasNext && requestBuffer.size < requestBufferSize) { //if the Ip is in the response already, just join it, and move to the next input
          val nextInput = input.next()                       //get the next input
          val nextIP = leftKeyGenerator(nextInput).getString(0) //and its IP
          val nextIPHash = nextIP.hashCode()
          if (responseCache.get(nextIPHash) != null) {
            toOutput.enqueue(new JoinedRow(nextInput, responseCache.get(nextIPHash)))
          } else {
            if (requestBuffer.get(nextIPHash) == null) {
              requestBuffer.put(nextIPHash, Row(nextIP, 1))
              DNSLookup.lookup(nextIPHash, nextIP, responseCache, requestBuffer)
            } else {
              requestBuffer.put(nextIPHash, Row(nextIP, requestBuffer.get(nextIPHash).getInt(1) + 1))
            }
            requests += 1
          }
        }
        //leftKeyGenerator(requestQueue.dequeue).getInt(0)
      }
    }
  }
}