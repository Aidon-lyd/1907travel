package com.qf.bigdata.realtime.flink.constant

/**
 * 实时场景使用常量类
 */
object QRealTimeConstant {

  //常用常数
  val COMMON_NUMBER_ZERO : Long = Long.box(0l)

  val COMMON_NUMBER_ZERO_INT : Int = Int.box(0)

  val COMMON_NUMBER_ONE : Long = Long.box(1l)

  val COMMON_AGG_ZERO : Double = Double.box(0.0d)

  val COMMON_MAX_COUNT : Long = Long.box(100)


  //kafka消费者配置文件
  val KAFKA_CONSUMER_CONFIG_URL = "kafka/flink/kafka-consumer.properties"
  //kafka生产者配置文件
  val KAFKA_PRODUCER_CONFIG_URL = "kafka/flink/kafka-producer.properties"

  //mysql属性文件
  val MYSQL_CONFIG_URL = "jdbc.properties"



  //flink最大乱序时间
  val FLINK_WATERMARK_MAXOUTOFORDERNESS = 5l

  //flink水位间隔时间
  val FLINK_WATERMARK_INTERVAL = 5 * 1000l


  //flink窗口大小
  val FLINK_WINDOW_SIZE :Long = 10L
  val FLINK_WINDOW_MAX_SIZE :Long = 60L

  //flink滑动窗口大小
  val FLINK_SLIDE_WINDOW_SIZE :Long= 10L
  val FLINK_SLIDE_INTERVAL_SIZE :Long= 5L

  //flink检查点间隔
  val FLINK_CHECKPOINT_INTERVAL :Long = 3 * 1000l

  //flink延时设置
  val FLINK_ALLOWED_LATENESS :Long = 10L


  //flink cep
  val FLINK_CEP_VIEW_BEGIN = "CEP_VIEW_BEGIN"


  //本地模型下的默认并行度(cpu core)
  val DEF_LOCAL_PARALLELISM  = Runtime.getRuntime.availableProcessors

  //flink服务重启策略相关参数
  val RESTART_ATTEMPTS :Int = 5
  val RESTART_DELAY_BETWEEN_ATTEMPTS :Long = 1000L * 10L

  //广播变量名称
  val BC_ACTIONS = "bc_actions"
  val BC_PRODUCT = "bc_product"
  val BC_PRODUCT_ASYNC = "bc_product_sync"

  //外部传参名称
  val PARAMS_KEYS_APPNAME = "appname"
  val PARAMS_KEYS_GROUPID = "gruopid"

  val PARAMS_KEYS_TOPIC_FROM = "from_topic"
  val PARAMS_KEYS_TOPIC_TO = "to_topic"

  val PARAMS_KEYS_INDEX_NAME = "index"
  val PARAMS_KEYS_REDIS_DB = "redis_db"


  //kafka参数
  val TOPIC_LOG_ODS = "travel_logs_ods"

  val TOPIC_LOG_ACTION_VIEW = "topic_log_action_view"
  val TOPIC_LOG_ACTION_VIEW_STATIS = "topic_log_action_view_statis"

  val TOPIC_LOG_ACTION_CLICK = "topic_log_action_click"
  val TOPIC_LOG_ACTION_LAUNCH = "topic_log_action_launch"
  val TOPIC_LOG_ACTION_PAGE_ENTER = "topic_log_action_page_enter"
  val TOPIC_LOG_ACTION_VIEWLIST = "topic_log_action_viewlist"

  val TOPIC_LOG_ACTION_LAUNCH_WARN = "topic_log_action_launch_warn"

  val TOPIC_ORDER_ODS = "travel_order_ods"
  val TOPIC_ORDER_DW = "travel_order_dw"
  val TOPIC_ORDER_DW_WIDE = "topic_orders_dw_wide"
  val TOPIC_ORDER_DM = "topic_orders_dm"
  val TOPIC_ORDER_DM_STATIS = "topic_orders_dm_statis"

  val TOPIC_ORDER_MID = "topic_orders_mid"

  //测流输出
  val OUTPUT_TAG_LOG_PAGEVIEW = "output_tag_pageview_lowDuration"

  //===es索引 旅游产品订单=====================================
  val ES_INDEX_NAME_ORDER_DETAIL = "travel_order_detail"
  val ES_INDEX_NAME_ORDER_AGG = "travel_order_agg"
  val ES_INDEX_NAME_ORDER_STATIS = "travel_order_statis"
  val ES_INDEX_NAME_ORDER_CUSTOMER_STATIS = "travel_order_customer_statis"

  val ES_INDEX_NAME_ORDER_WIDE_DETAIL = "travel_order_wide"
  val ES_INDEX_NAME_ORDER_WIDE_AGG = "travel_order_wide_agg"

  //===es索引 用户日志=====================================
  //启动日志聚合数据对应es索引名称
  val ES_INDEX_NAME_LOG_LAUNCH_AGG = "travel_log_launch_agg"

  //页面浏览日志明细数据对应es索引名称
  val ES_INDEX_NAME_LOG_VIEW = "travel_log_pageview"
  val ES_INDEX_NAME_LOG_VIEW_LOW = "travel_log_pageview_low"

  //用户日志点击行为明细数据对应es索引名称
  val ES_INDEX_NAME_LOG_CLICK = "travel_log_click"
  val ES_INDEX_NAME_LOG_CLICK_STATIS = "travel_log_click_statis"

  //ES集群配置文件
  val ES_CONFIG_PATH = "es/es-config.json"


  //ES同记录写入并发重试次数
  val ES_RETRY_NUMBER = 15

  //ES索引中的时间列
  val esCt = "es_ct"
  val esUt = "es_ut"

  val ES_PV = "view_count"
  val ES_MAX = "max_metric"
  val ES_MIN = "min_metric"

  val ES_ORDERS = "orders"

  val KEY_ES_ID = "id"

  val DYNC_DBCONN_TIMEOUT = 1
  val DYNC_DBCONN_CAPACITY = 3


  //时间格式
  val FORMATTER_YYYYMMDD: String = "yyyyMMdd"
  val FORMATTER_YYYYMMDD_MID: String = "yyyy-MM-dd"
  val FORMATTER_YYYYMMDDHH: String = "yyyyMMddHH"
  val FORMATTER_YYYYMMDDHHMMSS: String = "yyyyMMddHHmmss"

  //实时采集参数
  val RM_REC_ROLLOVER_INTERVAL :Long = 10L
  val RM_REC_INACTIVITY_INTERVAL :Long = 20L
  val RM_REC_MAXSIZE :Long = 128L
  val RM_REC_BUCKET_CHECK_INTERVAL :Long = 5L

  val KEY_RM_REC_OUTPUT :String = "output"
  val KEY_RM_REC_ROLLOVER_INTERVAL :String = "rollover_interval"
  val KEY_RM_REC_INACTIVITY_INTERVAL :String = "inactivity_interval"
  val KEY_RM_REC_MAXSIZE :String = "max_size"
  val KEY_RM_REC_BUCKET_CHECK_INTERVAL :String = "bucket_check_interval"

  val KEY_RM_TIME_RANGE :String = "time_range"
  val KEY_RM_LAUNCH_COUNT :String = "launch_count"


  val PATTERN_LAUNCH_USER:String = "pattern_launch_user"

  //======================================================================

  val POJO_FIELD_PRODUCTID = "productID"
  val POJO_FIELD_USERREGION = "userRegion"
  val POJO_FIELD_TRAFFIC = "traffic"
  val POJO_FIELD_TRAFFIC_GRADE = "trafficGrade"
  val POJO_FIELD_FEE = "fee"
  val POJO_FIELD_FEE_MAX = "maxFee"
  val POJO_FIELD_FEE_MIN = "minFee"
  val POJO_FIELD_MEMBERS = "members"
  val POJO_FIELD_ORDERS = "orders"
  val POJO_PRODUCT_DEPCODE = "depCode"
  val POJO_FIELD_PRODUCT_TYPE = "productType"
  val POJO_FIELD_TOURSIM_TYPE = "toursimType"



  //=====基础信息==============================================================
  //kafka分区Key
  val KEY_KAFKA_ID = "KAFKA_ID"

  //请求ID
  val KEY_SID = "sid"
  //用户ID
  val KEY_USER_ID = "userID"
  //用户设备号
  val KEY_USER_DEVICE = "userDevice"
  //终端类型
  val KEY_CONSOLE_TYPE = "consoleType"
  //用户设备类型
  val KEY_USER_DEVICE_TYPE = "userDeviceType"
  //操作系统
  val KEY_OS = "os"
  //手机制造商
  val KEY_MANUFACTURER = "manufacturer"
  //电信运营商
  val KEY_CARRIER = "carrier"
  //网络类型
  val KEY_NETWORK_TYPE = "networkType"
  //用户所在地区
  val KEY_USER_REGION = "userRegion"
  //用户所在地区IP
  val KEY_USER_REGION_IP = "userRegionIP"
  //经度
  val KEY_LONGITUDE = "lonitude"
  //纬度
  val KEY_LATITUDE = "latitude"

  //行为类型
  val KEY_ACTION = "action"

  //事件类型
  val KEY_EVENT_TYPE = "eventType"

  //停留时长
  val KEY_DURATION = "duration"

  //事件目的
  val KEY_EVENT_TARGET_TYPE = "eventTargetType"

  //热门目的地
  val KEY_HOT_TARGET = "hotTarget"

  //目标页面集合
  val KEY_TARGET_ID = "targetID"
  val KEY_TARGET_IDS = "targetIDS"

  // 出发时间
  val KEY_TRAVEL_SENDTIME = "travelSendTime"

  //行程天数
  val KEY_TRAVEL_TIME = "travelTime"

  // 产品钻级
  val KEY_PRODUCT_LEVEL = "productLevel"

  //出发地
  val KEY_TRAVEL_SEND = "travelSend"

  // 产品类型：跟团、私家、半自助
  val KEY_PRODUCT_TYPE = "productType"



  //===订单业务======================================================
  //订单id
  val KEY_ORDER_ID = "order_id"

  //酒店ID
  val KEY_PUB_ID = "product_pub"

  //用户手机
  val KEY_USER_MOBILE = "user_mobile"

  //交通出行方式(高铁、飞机...)
  val KEY_TRAFFIC = "product_traffic"

  //交通出行等级:座席
  val KEY_TRAFFIC_GRADE = "product_traffic_grade"

  //交通出行种类(单程|往返)
  val KEY_TRAFFIC_TYPE = "product_traffic_type"

  //价格
  val KEY_PRODUCT_PRICE = "product_price"

  //花费
  val KEY_PRODUCT_FEE = "product_fee"

  //折扣
  val KEY_HAS_ACTIVITY = "has_activity"

  //成人、儿童、婴儿
  val KEY_PRODUCT_ADULT = "travel_member_adult"
  val KEY_PRODUCT_YONGER = "travel_member_yonger"
  val KEY_PRODUCT_BABY = "travel_member_baby"

  //产品维度信息
  val KEY_PRODUCT_TOURSIMTYPE = "toursimType"
  val KEY_PRODUCT_DEPCODE = "depCode"
  val KEY_PRODUCT_DESCODE = "desCode"



  //下单时间
  val KEY_ORDER_CT = "order_ct"

  //==========================================================
  val DURATION_SIDEOUT = 5

  val KEY_DURATION_PAGE_NAGV_MIN = 3
  val KEY_DURATION_PAGE_NAGV_MAX = 90

  val KEY_DURATION_PAGE_VIEW_MIN = 3
  val KEY_DURATION_PAGE_VIEW_MAX = 60

  //创建时间
  val KEY_CT = "ct"
  //更新时间
  val KEY_UT = "ut"
  //计算时间
  val KEY_PT = "pt"


  //=====扩展信息==============================================================

  val KEY_EXTS = "exts"

  //产品ID
  val KEY_PRODUCT_ID = "product_id"

  //产品列表
  val KEY_PRODUCT_IDS = "product_ids"

  //=====查询信息==============================================================


  //用户数量限制级别
  val USER_COUNT_LEVEL = 5

  //================================================================

  //MYSQL维表列名
  val MYSQL_FIELD_PRODUCT_ID = "product_id"
  val MYSQL_FIELD_PUB_ID = "pub_id"

  //产品维表
  val MYDQL_DIM_PRODUCT = "travel.dim_product1"
  val SQL_PRODUCT = s"""
        select
                       |product_id,
                       |product_level,
                       |product_type,
                       |departure_code,
                       |des_city_code,
                       |toursim_tickets_type
                       |from ${MYDQL_DIM_PRODUCT}
      """.stripMargin


  val SCHEMA_PRODUCT = "product_id, product_level, product_type, departure_code, des_city_code, toursim_tickets_type"



  //酒店维表
  val MYDQL_DIM_PUB = "travel.dim_pub1"
  val SQL_PUB = s"""
        select
                   |pub_id,
                   |pub_name,
                   |pub_star,
                   |pub_grade,
                   |is_national
        from ${MYDQL_DIM_PUB}
      """.stripMargin

  val SCHEMA_PUB = "pub_id,pub_name,pub_star,pub_grade,is_national"


  //===Redis conf================================
  val REDIS_CONF_HOST = "redis_host"
  val REDIS_CONF_PASSWORD = "redis_password"
  val REDIS_CONF_TIMEOUT = "redis_timeout"
  val REDIS_CONF_PORT = "redis_port"
  val REDIS_CONF_DB = "redis_db"
  val REDIS_CONF_MAXIDLE = "redis_maxidle"
  val REDIS_CONF_MINIDLE = "redis_minidle"
  val REDIS_CONF_MAXTOTAL = "redis_maxtotal"

  val REDIS_CONF_PATH = "redis/redis.properties"

  val REDIS_DB = 0


  //flink的jdbc的参数
  val FLINK_JDBC_DRIVER_MYSQL_KEY = "jdbc.driver"
  val FLINK_JDBC_URL_KEY = "jdbc.url";
  val FLINK_JDBC_USERNAME_KEY = "jdbc.user";
  val FLINK_JDBC_PASSWD_KEY = "jdbc.password";

  //符号常量
  val BOTTOM_LINE = "_"
  val COMMA = ","
  val CT = "ct"
}
