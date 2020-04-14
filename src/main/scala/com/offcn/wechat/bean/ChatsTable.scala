package com.offcn.wechat.bean

case class ChatsTable(
						 topicType: String,
						 entityId: String,
						 entityTyp: String,
						 isAdded: Boolean,
						 createTime: String,
						 isSend: Int,
						 msgSvrId: String,
						 source: String,
						 talker: String,
						 wxtype: Int
					 )
