/**
  * Created by zhangfanfan on 2019/1/25.
  */


import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.apache.spark.sql.SparkSession
import org.mortbay.jetty.{HttpStatus, Request, Server}
import org.mortbay.jetty.handler._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

import org.bson.Document
import org.bson.types.ObjectId
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.MongoSpark

object MongoApp extends AbstractHandler{
  /**
    * 处理请求 返回响应
    * @param target
    * @param request
    * @param response
    * @param dispatch
    */

  private[this] var  m_session: SparkSession = _;
  private [this] var m_args: Array[String] = _;

  override def handle(target: String,
                      request: HttpServletRequest,
                      response: HttpServletResponse,
                      dispatch: Int): Unit = {
    val url=request.getRequestURI
    url.substring(url.lastIndexOf("/")+1,url.length) match {
      case "recommend" => {
        //request中的target 用,号分割
        //val target: Seq[String] = request.getParameter("target").split(",").toSeq
        //val topNum: Int = request.getParameter("topNum").toInt
        //val result = model.recommend(target, topNum)


        val uri: String = m_args.headOption.getOrElse("mongodb://marsdapp:marsdapp2019@172.17.156.133:27100/tron.block1_100000?authSource=admin")
        //val uri: String = args.headOption.getOrElse("mongodb://172.17.156.133:27017/tron.contract")
        val conf = new SparkConf()
          .setMaster("spark://master:7077")
          .setAppName("DApp")
          .set("spark.app.id", "DApp")
          .set("spark.mongodb.input.uri", uri)
          .set("spark.mongodb.output.uri", uri)

        val session = SparkSession.builder().config(conf).getOrCreate()

        val df = MongoSpark.load(session)
        df.createOrReplaceTempView("block")


        val block = session.sql("SELECT * from block ")
        val count = block.count()

        response.setStatus(HttpStatus.ORDINAL_200_OK);
        val result = "count is %s".format(count)

        //response.getWriter().println(result.mkString(","))
        response.getWriter().println(result)
        request.asInstanceOf[Request].setHandled(true)
        response.getWriter.close()


      }
      case "recommend1" =>{

        //val df = MongoSpark.load(m_session)
        //df.createOrReplaceTempView("block")
        //val block = m_session.sql("SELECT * from block")
        //val first = block.first()
        //val aa = first.getInt(3)

        println("--------------------------------------------")

        val uri: String = m_args.headOption.getOrElse("mongodb://marsdapp:marsdapp2019@172.17.156.133:27100/tron.contract?authSource=admin")
        //val uri: String = args.headOption.getOrElse("mongodb://172.17.156.133:27017/tron.contract")
        val conf = new SparkConf()
          .setMaster("spark://master:7077")
          .setAppName("DApp")
          .set("spark.app.id", "DApp")
          .set("spark.mongodb.input.uri", uri)
          .set("spark.mongodb.output.uri", uri)

        val session = SparkSession.builder().config(conf).getOrCreate()
        val df = MongoSpark.load(session)

        //print(customRdd.count())

        df.createOrReplaceTempView("transactions")
        val transaction = session.sql("select * from transactions")
        val count1 = transaction.count()
        //val bb = tran_fist.getString(2)

        val result = "count is %d".format(count1)
        //val result = "ccccc"
        response.setStatus(HttpStatus.ORDINAL_200_OK);
        response.getWriter().println(result)
        request.asInstanceOf[Request].setHandled(true)
        response.getWriter.close()

      }
      case "recommend2" =>{


        val uri: String = m_args.headOption.getOrElse("mongodb://marsdapp:marsdapp2019@172.17.156.133:27100/tron.contract?authSource=admin")
        //val uri: String = args.headOption.getOrElse("mongodb://172.17.156.133:27017/tron.contract")
        val conf = new SparkConf()
          .setMaster("spark://master:7077")
          .setAppName("DApp")
          .set("spark.app.id", "DApp")
          .set("spark.mongodb.input.uri", uri)
          .set("spark.mongodb.output.uri", uri)

        val sc = SparkSession.builder().config(conf).getOrCreate()
        //val readConfig = ReadConfig(Map("collection" -> "contract", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
        val readConfig = ReadConfig(Map("collection" -> "contract", "readPreference.name" -> "secondaryPreferred"))
        val customRdd = MongoSpark.load(sc, readConfig)

        customRdd.createOrReplaceTempView("contract")

        val contract = sc.sql("select * from contract")
        println("--------------------------------aaaaa")
        println(contract.count())

        /////
        //val readConfig1 = ReadConfig(Map("collection" -> "block1_100000", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
        val readConfig1 = ReadConfig(Map("collection" -> "block1_100000", "readPreference.name" -> "secondaryPreferred"))

        val customRdd1 = MongoSpark.load(sc, readConfig1)

        customRdd1.createOrReplaceTempView("block1_100000")

        val block1_100000 = sc.sql("select * from block1_100000")
        println("--------------------------------bbbbb")
        println(block1_100000.count())


        val result = "(block1_100000.count() is %d".format(block1_100000.count())
        //val result = "ccccc"
        response.setStatus(HttpStatus.ORDINAL_200_OK);
        response.getWriter().println(result)
        request.asInstanceOf[Request].setHandled(true)
        response.getWriter.close()


      }
      case "recommend3" =>{
        val uri: String = m_args.headOption.getOrElse("mongodb://marsdapp:marsdapp2019@172.17.156.133:27100/tron.block1_100000?authSource=admin")
        //val uri: String = args.headOption.getOrElse("mongodb://172.17.156.133:27017/tron.contract")
        val conf = new SparkConf()
          .setMaster("spark://master:7077")
          .setAppName("DApp")
          .set("spark.app.id", "DApp")
          .set("spark.mongodb.input.uri", uri)
          .set("spark.mongodb.output.uri", uri)
          .set("spark.mongodb.input.database","tron")

        val session = SparkSession.builder().config(conf).getOrCreate()
        //val readConfig = ReadConfig(Map("collection" -> "block1_100000", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(session)))
        val readConfig = ReadConfig(Map("uri" -> uri ,"database"  -> "tron" , "collection" -> "block1_100000", "readPreference.name" -> "secondaryPreferred"))

        val df = MongoSpark.load(session,readConfig)
        df.createOrReplaceTempView("block")


        val block = session.sql("SELECT hash,number,witness_address from block ")
        println("--------------------------------cccc")
        println(block.first().getString(2))

        val result = "(witness_address is %s".format(block.first().getString(2))
        //val result = "ccccc"
        response.setStatus(HttpStatus.ORDINAL_200_OK);
        response.getWriter().println(result)
        request.asInstanceOf[Request].setHandled(true)
        response.getWriter.close()
      }
      case "contract" =>{
        val uri: String = m_args.headOption.getOrElse("mongodb://marsdapp:marsdapp2019@172.17.156.133:27100/tron.transaction6000001_6100000?authSource=admin")
        //val uri: String = args.headOption.getOrElse("mongodb://172.17.156.133:27017/tron.contract")
        val conf = new SparkConf()
          .setMaster("spark://master:7077")
          .setAppName("DApp")
          .set("spark.app.id", "DApp")
          .set("spark.mongodb.input.uri", uri)
          .set("spark.mongodb.output.uri", uri)
          .set("spark.mongodb.input.database","tron")
          .set("spark.mongodb.input.partitioner", "MongoSplitVectorPartitioner")

        val session = SparkSession.builder().config(conf).getOrCreate()
        val readConfig = ReadConfig(Map("uri" -> uri, "partitioner" -> "MongoSplitVectorPartitioner", "database"  -> "tron", "collection" -> "transaction6000001_6100000", "readPreference.name" -> "secondaryPreferred"))
       // val readConfig = ReadConfig(Map("collection" -> "transaction6000001_6100000", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(session)))

        val df = MongoSpark.load(session,readConfig)
        df.createOrReplaceTempView("transaction6000001_6100000")


        //val block = session.sql("SELECT hash,contract_type,contract_owner_address,contract_address from transaction WHERE contract_type='TriggerSmartContract'")
        //val block = session.sql("SELECT hash,contract_type,contract_owner_address,contract_address,COUNT(1) from transaction GROUP BY contract_address HAVING contract_type='TriggerSmartContract'")
        val block = session.sql("SELECT hash,contract_type,contract_owner_address,block_number from transaction6000001_6100000 WHERE block_number > 6090010 ")

        println("--------------------------------contract")
        println(block.count())

        val result = "OK"
        //val result = "ccccc"
        response.setStatus(HttpStatus.ORDINAL_200_OK);
        response.getWriter().println(result)
        request.asInstanceOf[Request].setHandled(true)
        response.getWriter.close()
      }
      case "contract1" =>
      {
        val uri: String = m_args.headOption.getOrElse("mongodb://marsdapp:marsdapp2019@172.17.156.133:27100/tron.transaction6000001_6100000?authSource=admin")
        //val uri: String = args.headOption.getOrElse("mongodb://172.17.156.133:27017/tron.contract")
        val conf = new SparkConf()
          .setMaster("spark://master:7077")
          .setAppName("DApp")
          .set("spark.app.id", "DApp")
          .set("spark.mongodb.input.uri", uri)
          .set("spark.mongodb.output.uri", uri)
          .set("spark.mongodb.input.database","tron")
          .set("spark.mongodb.input.partitioner", "MongoSplitVectorPartitioner")

        val session = SparkSession.builder().config(conf).getOrCreate()
        val readConfig = ReadConfig(Map("uri" -> uri, "partitioner" -> "MongoSplitVectorPartitioner", "database"  -> "tron", "collection" -> "transaction6000001_6100000", "readPreference.name" -> "secondaryPreferred"))
        // val readConfig = ReadConfig(Map("collection" -> "transaction6000001_6100000", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(session)))

        val df = MongoSpark.load(session,readConfig)
        df.createOrReplaceTempView("transaction6000001_6100000")


        //val block = session.sql("SELECT hash,contract_type,contract_owner_address,contract_address from transaction WHERE contract_type='TriggerSmartContract'")
        val block = session.sql("SELECT collect_set(contract_type),collect_set(contract_owner_address),collect_set(contract_address) ,COUNT(*) from transaction6000001_6100000  WHERE contract_type='TriggerSmartContract' GROUP BY contract_address")
        //val block = session.sql("SELECT hash,contract_type,contract_owner_address,block_number from transaction6000001_6100000 WHERE  contract_type='TriggerSmartContract' ")

        println("--------------------------------contract contract_type")
        //println(block.count())
        println(block.first().getLong(3))
        val result = "OK"
        //val result = "ccccc"
        response.setStatus(HttpStatus.ORDINAL_200_OK);
        response.getWriter().println(result)
        request.asInstanceOf[Request].setHandled(true)
        response.getWriter.close()
      }
      case "contract2" =>
      {
        val uri: String = m_args.headOption.getOrElse("mongodb://marsdapp:marsdapp2019@172.17.156.133:27100/tron.transaction6000001_6100000?authSource=admin")
        //val uri: String = args.headOption.getOrElse("mongodb://172.17.156.133:27017/tron.contract")
        val conf = new SparkConf()
          .setMaster("spark://master:7077")
          .setAppName("DApp")
          .set("spark.app.id", "DApp")
          .set("spark.mongodb.input.uri", uri)
          .set("spark.mongodb.output.uri", uri)
          .set("spark.mongodb.input.database","tron")
          .set("spark.mongodb.input.partitioner", "MongoSplitVectorPartitioner")

        val session = SparkSession.builder().config(conf).getOrCreate()
        val readConfig = ReadConfig(Map("uri" -> uri, "partitioner" -> "MongoSplitVectorPartitioner", "database"  -> "tron", "collection" -> "transaction6000001_6100000", "readPreference.name" -> "secondaryPreferred"))
        // val readConfig = ReadConfig(Map("collection" -> "transaction6000001_6100000", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(session)))

        val df = MongoSpark.load(session,readConfig)
        df.createOrReplaceTempView("transaction6000001_6100000")
        df.createOrReplaceTempView("transaction6100001_6200000")


        //val block = session.sql("SELECT hash,contract_type,contract_owner_address,contract_address from transaction WHERE contract_type='TriggerSmartContract'")
        val block = session.sql("SELECT collect_set(contract_address) ,COUNT(*) from transaction6000001_6100000  WHERE contract_type='TriggerSmartContract' UNION SELECT collect_set(contract_address) ,COUNT(*) from transaction6100001_6200000  WHERE contract_type='TriggerSmartContract' GROUP BY contract_address")
        //val block = session.sql("SELECT hash,contract_type,contract_owner_address,block_number from transaction6000001_6100000 WHERE  contract_type='TriggerSmartContract' ")

        println("--------------------------------contract contract_type")
        //println(block.count())
        println(block.first().getLong(1))
        val result = "OK"
        //val result = "ccccc"
        response.setStatus(HttpStatus.ORDINAL_200_OK);
        response.getWriter().println(result)
        request.asInstanceOf[Request].setHandled(true)
        response.getWriter.close()
      }
      case _ => {
        response.setStatus(HttpStatus.ORDINAL_404_Not_Found);
        request.asInstanceOf[Request].setHandled(true)
      }
    }
  }

  def main(args: Array[String]): Unit = {

    /*
    val uri: String = args.headOption.getOrElse("mongodb://marsdapp:marsdapp2019@172.17.156.133:27100/tron.block1_100000?authSource=admin")
    //val uri: String = args.headOption.getOrElse("mongodb://172.17.156.133:27017/tron.contract")
    val conf = new SparkConf()
      .setMaster("spark://master:7077")
      .setAppName("DApp")
      .set("spark.app.id", "DApp")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)

    val session = SparkSession.builder().config(conf).getOrCreate()
    m_session = session*/
    m_args = args

    val server=new Server(9998)
    server.setHandler(this)
    server.start()

  }
}
