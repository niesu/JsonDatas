package com.offcn.wechat.bean

case class Mement_ContentTable(

								  topicType: String,
								  entityId: String,
								  entityTyp: String,
								  id: String,
								  authorid: String,
								  authorname: String,
								  content: String,
								  ts: String,
								  wxtype: String,

								  likes_userName: String, //点赞人名字
								  likes_userId: String, //点赞人微信ID
								  likes_isCurrentUser: Boolean, //是否本人

								  mediaurls: String, //资源路径

								  comments_authorName: String, //评论人名字
								  comments_authorId: String, //评论人微信ID
								  comments_content: String, //评论内容
								  comments_isCurrentUser: Boolean //是否是本人
							  )
