package com.offcn.wechat.bean

case class ConversationTable(topicType: String,
							 entityId: String,
							 entityTyp: String,
							 id: String,
							 createTime: String,
							 wx_id: String,
							 totalCount: Int,
							 unReadCount: Int)
