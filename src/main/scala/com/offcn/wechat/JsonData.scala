package com.offcn.wechat


import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.{lang, util}
import java.util.Map

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.offcn.util.ParseJsonData
import com.offcn.wechat.bean.{BaseData, BaseTable, ChatsTable, ConversationTable, GroupTable, Mement_CommentsnsTable, Mement_ContentTable, Mement_LikesnsTable, UserTable}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer


object JsonData {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder().appName("JsonParse").master("local[*]").getOrCreate()

		val stringDS: Dataset[String] = spark.read.textFile("Data/json")
		//		df.printSchema()
		//		df.createOrReplaceTempView("a")
		//		df.show()
		import spark.implicits._

		val btDS: Dataset[BaseTable] = stringDS.mapPartitions(partition => {
			partition.map(line => {
				val jsonObject: JSONObject = ParseJsonData.getJsonData(line)
				val topicType: String = jsonObject.getString("topicType")
				val entityId: String = jsonObject.getString("entityId")
				val entityTyp: String = jsonObject.getString("entityTyp")
				val exts: String = jsonObject.getJSONObject("exts").toString

				//			val set: util.Set[Map.Entry[String, AnyRef]] = exts.entrySet()
				//			val map: Map.Entry[String, AnyRef] = set.iterator().next()
				//			val key: String = map.getKey
				//			val value = map.getValue.toString
				//			BaseData(key, value)
				BaseTable(topicType, entityId, entityTyp, exts)
			})
		})

		// btDS.toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("ods.baseTable")
		btDS.createOrReplaceTempView("baseTable")
		val tableDF = spark.sql("select * from baseTable")

		// WECHAT_GROUP
		val GroupDS: Dataset[GroupTable] = tableDF.mapPartitions(partition => {
			partition.filter(_.getAs[String]("topicType") == "WECHAT_GROUP")
				.flatMap(row => {
					val topicType: String = row.getAs[String]("topicType")
					val entityId: String = row.getAs[String]("entityId")
					val entityTyp: String = row.getAs[String]("entityTyp")
					//val exts: String = row.getString(3)
					val exts: String = row.getAs[String]("exts")

					val eObject: JSONObject = ParseJsonData.getJsonData(exts)
					val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
					val array: JSONArray = eObject.getJSONArray("groupinfo")
					val list: ListBuffer[GroupTable] = ListBuffer[GroupTable]()
					val arr: Array[AnyRef] = array.toArray
					for (elem <- arr) {

						val sObject: JSONObject = elem.asInstanceOf[JSONObject]
						val conversationTime: Timestamp = sObject.getTimestamp("conversationTime")
						val time: String = format.format(conversationTime)
						val id: String = sObject.getString("id")
						val img: String = sObject.getString("img")
						val msgCount: String = sObject.getInteger("msgCount").toString
						val name: String = sObject.getString("name")
						val owner: String = sObject.getString("owner")
						val unReadCount: String = sObject.getInteger("unReadCount").toString


						list.append(GroupTable(topicType, entityId, entityTyp, time, id, img, msgCount, name, owner, unReadCount))
					}
					list
				})
		})
		GroupDS.write.format("csv").save("Data/group.csv")

		// WECHAT_CONVERSATION
		val converDS: Dataset[ConversationTable] = tableDF.mapPartitions(partition => {
			partition.filter(_.getAs[String]("topicType") == "WECHAT_CONVERSATION")
				.flatMap(row => {
					val topicType: String = row.getAs[String]("topicType")
					val entityId: String = row.getAs[String]("entityId")
					val entityTyp: String = row.getAs[String]("entityTyp")
					val exts: String = row.getAs[String]("exts")

					val eObject: JSONObject = ParseJsonData.getJsonData(exts)
					val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
					val conversation: JSONArray = eObject.getJSONArray("conversation")
					val converList: ListBuffer[ConversationTable] = ListBuffer[ConversationTable]()

					val arr: Array[AnyRef] = conversation.toArray
					val info: AnyRef = arr.toIterator.next()

					val infoObject: JSONObject = info.asInstanceOf[JSONObject]
					val id: String = infoObject.getString("id")
					val infoDTO: JSONArray = infoObject.getJSONArray("conversationInfoDTO")
					val infoDTOArray: Array[AnyRef] = infoDTO.toArray

					for (elem <- infoDTOArray) {

						val sObject: JSONObject = elem.asInstanceOf[JSONObject]
						val createTime: Timestamp = sObject.getTimestamp("createTime")
						val time: String = format.format(createTime)
						val wx_id: String = sObject.getString("id")
						val totalCount: Int = sObject.getInteger("totalCount").intValue()
						val unReadCount: Int = sObject.getInteger("unReadCount").intValue()

						converList.append(ConversationTable(topicType, entityId, entityTyp, id, time, wx_id, totalCount, unReadCount))
					}
					converList
				})
		})
		converDS.write.format("csv").save("Data/conver.csv")

		// WECHAT_USER
		val userDS: Dataset[UserTable] = tableDF.mapPartitions(partition => {
			partition.filter(_.getAs[String]("topicType") == "WECHAT_USER")
				.flatMap(row => {
					val topicType: String = row.getAs[String]("topicType")
					val entityId: String = row.getAs[String]("entityId")
					val entityTyp: String = row.getAs[String]("entityTyp")
					val exts: String = row.getAs[String]("exts")

					val eObject: JSONObject = ParseJsonData.getJsonData(exts)
					val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
					val users: JSONArray = eObject.getJSONArray("users")
					val usersList: ListBuffer[UserTable] = ListBuffer[UserTable]()

					val userArr: Iterator[AnyRef] = users.toArray.toIterator
					val counsellorsArr: AnyRef = userArr.next()

					val counsellors: JSONObject = counsellorsArr.asInstanceOf[JSONObject]
					val androidVersion: String = counsellors.getString("androidVersion")
					val area: String = counsellors.getString("area")
					val areas: String = counsellors.getJSONArray("areas").toString
					val corpid: String = counsellors.getString("corpid")
					val departme: String = counsellors.getString("departme")
					val deviceBrand: String = counsellors.getString("deviceBrand")
					val headImg: String = counsellors.getString("headImg")
					val sells_id: String = counsellors.getString("id")
					val imei: String = counsellors.getString("imei")
					val isRoot: lang.Boolean = counsellors.getBoolean("isRoot")
					val jobArray: JSONArray = counsellors.getJSONArray("jobList")
					var jobString: String = null
					val jobInter: util.Iterator[AnyRef] = jobArray.iterator()
					while (jobInter.hasNext) {
						jobString = jobString + "," + jobInter.next().toString
					}

					val mobile: String = counsellors.getString("mobile")
					val nick: String = counsellors.getString("nick")
					val operateType: Int = counsellors.getInteger("operateType").intValue()
					val parentsArray: JSONArray = counsellors.getJSONArray("parents")
					val parentsIter: util.Iterator[AnyRef] = parentsArray.iterator()
					var parentsString: String = null
					while (parentsIter.hasNext) {
						parentsString = parentsString + "," + parentsIter.next().toString
					}

					val parentsHistoryArray: JSONArray = counsellors.getJSONArray("parentsHistory")
					val parentsHistoryIter: util.Iterator[AnyRef] = parentsHistoryArray.iterator()
					var parentsHistoryString: String = null
					while (parentsHistoryIter.hasNext) {
						parentsHistoryString = parentsHistoryString + "," + parentsHistoryIter.next().toString
					}

					val realnam: String = counsellors.getString("realnam")
					val remark: String = counsellors.getString("remark")
					val remarks: String = counsellors.getJSONArray("remarks").toString
					val softVersion: String = counsellors.getString("softVersion")
					val systemModel: String = counsellors.getString("systemModel")
					val systemVersion: String = counsellors.getString("systemVersion")
					val wxtype: String = counsellors.getString("type")
					val wx_name: String = counsellors.getString("wx_name")

					usersList.append(UserTable(topicType,
						entityId,
						entityTyp,
						sells_id,
						null,
						androidVersion,
						area,
						areas,
						corpid,
						departme,
						deviceBrand,
						headImg,
						imei,
						isRoot,
						jobString,
						mobile,
						nick,
						operateType,
						parentsString,
						parentsHistoryString,
						realnam,
						remark,
						remarks,
						softVersion,
						systemModel,
						systemVersion,
						wxtype,
						wx_name
					))

					while (userArr.hasNext) {
						val user: AnyRef = userArr.next()
						val users: JSONObject = user.asInstanceOf[JSONObject]

						val _areas: String = users.getJSONArray("areas").toString
						val _headImg: String = users.getString("headImg")
						val _id: String = users.getString("id")
						val _isRoot: lang.Boolean = users.getBoolean("isRoot")
						val jobArray: JSONArray = users.getJSONArray("jobList")
						var _jobString: String = null
						val jobIter: util.Iterator[AnyRef] = jobArray.iterator()
						while (jobIter.hasNext) {
							_jobString = _jobString + "," + jobIter.next().toString
						}
						val _nick: String = users.getString("nick")
						val _operateType: Int = users.getInteger("operateType").intValue()
						val _parentsArray: JSONArray = users.getJSONArray("parents")
						val _parentsIter: util.Iterator[AnyRef] = _parentsArray.iterator()
						var _parentsString: String = null
						while (_parentsIter.hasNext) {
							_parentsString = _parentsString + "," + _parentsIter.next().toString
						}

						val _parentsHistoryArray: JSONArray = users.getJSONArray("parentsHistory")
						val _parentsHistoryIter: util.Iterator[AnyRef] = _parentsHistoryArray.iterator()
						var _parentsHistoryString: String = null
						while (_parentsHistoryIter.hasNext) {
							_parentsHistoryString = _parentsHistoryString + "," + _parentsHistoryIter.next().toString
						}

						val _remark: String = users.getString("remark")
						val _remarks: String = users.getJSONArray("remarks").toString
						val _wxtype: String = users.getString("type")
						val _wx_name: String = users.getString("wx_name")

						usersList.append(UserTable(topicType,
							entityId,
							entityTyp,
							null,
							_id,
							null,
							null,
							_areas,
							null,
							null,
							null,
							_headImg,
							null,
							_isRoot,
							_jobString,
							null,
							_nick,
							_operateType,
							_parentsString,
							_parentsHistoryString,
							null,
							_remark,
							_remarks,
							null,
							null,
							null,
							_wxtype,
							_wx_name
						))
					}
					usersList
				})
		})
		userDS.write.format("csv").save("Data/user.csv")

		// WECHAT_CHAT
		val chatsDS: Dataset[ChatsTable] = tableDF.mapPartitions(partition => {
			partition.filter(_.getAs[String]("topicType") == "WECHAT_CHAT")
				.flatMap(row => {
					val topicType: String = row.getAs[String]("topicType")
					val entityId: String = row.getAs[String]("entityId")
					val entityTyp: String = row.getAs[String]("entityTyp")
					val exts: String = row.getAs[String]("exts")

					val eObject: JSONObject = ParseJsonData.getJsonData(exts)
					val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
					val chats: JSONArray = eObject.getJSONArray("chats")
					val isAdded: lang.Boolean = eObject.getBoolean("isAdded")
					val userList: ListBuffer[ChatsTable] = ListBuffer[ChatsTable]()

					val chatsArray: Array[AnyRef] = chats.toArray
					for (chat <- chatsArray) {

						val sObject: JSONObject = chat.asInstanceOf[JSONObject]
						val content: String = sObject.getString("content")
						val createTime: Timestamp = sObject.getTimestamp("createTime")
						val time: String = format.format(createTime)
						val isSend: Int = sObject.getIntValue("isSend")
						val msgSvrId: String = sObject.getString("msgSvrId")
						val source: String = sObject.getString("source")
						val talker: String = sObject.getString("talker")
						val wxtype: Int = sObject.getIntValue("type")

						userList.append(ChatsTable(topicType, entityId, entityTyp, isAdded, time, isSend, msgSvrId, source, talker, wxtype))
					}
					userList
				})
		})
		chatsDS.write.format("csv").save("Data/chats.csv")

		//TODO WECHAT_MEMENT
//		tableDF.mapPartitions(partition => {
//			partition.filter(_.getAs[String]("topicType") == "WECHAT_MEMENT")
//				.flatMap(row => {
//					val topicType: String = row.getAs[String]("topicType")
//					val entityId: String = row.getAs[String]("entityId")
//					val entityTyp: String = row.getAs[String]("entityTyp")
//					val exts: String = row.getAs[String]("exts")
//
//					val eObject: JSONObject = ParseJsonData.getJsonData(exts)
//					val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//					val momentObject: JSONObject = eObject.getJSONObject("moment")
//					val id: String = momentObject.getString("id")
//
//					val commentsns: JSONArray = momentObject.getJSONArray("commentsns")
//					val contents: JSONArray = momentObject.getJSONArray("content")
//					val likes: JSONArray = momentObject.getJSONArray("likesns")
//
//					val commentsnsList: ListBuffer[Mement_CommentsnsTable] = ListBuffer[Mement_CommentsnsTable]()
//					val likesnsList: ListBuffer[Mement_LikesnsTable] = ListBuffer[Mement_LikesnsTable]()
//					val contentnsList: ListBuffer[Mement_ContentTable] = ListBuffer[Mement_ContentTable]()
//
//					val commentsnsArray: Array[AnyRef] = commentsns.toArray
//					val content: Array[AnyRef] = contents.toArray
//					val likeses: Array[AnyRef] = likes.toArray
//
//					if ()
//					for (commentsns <- commentsnsArray) {
//						val sObject: JSONObject = commentsns.asInstanceOf[JSONObject]
//						val content: String = sObject.getString("content")
//						val createTime: Timestamp = sObject.getTimestamp("createTime")
//						val time: String = format.format(createTime)
//						val isSend: Int = sObject.getIntValue("isSend")
//						val msgSvrId: String = sObject.getString("msgSvrId")
//						val source: String = sObject.getString("source")
//						val talker: String = sObject.getString("talker")
//						val wxtype: Int = sObject.getIntValue("type")
//
//						commentsnsList.append()
//					}
//					commentsnsList
//				})
//		})

	}

	//		spark.stop()
}

